//! The client against a peer whose every frame is scripted, for orders of events a real peer shows
//! only now and then.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use coqui_a2a::model::{Message, Role, SendMessageRequest, SendMessageResponse};
use coqui_a2a::{Client, ClientError};
use coqui_agentic_peer::mux::{CloseReason, Frame, Limits, Mux, Side};
use coqui_transport::{Channel, Failure};
use serde_json::json;

/// What the peer does when the client's request arrives.
#[derive(Clone, Copy)]
enum Script {
    /// Answers, finishes and closes, all before the client's `send` returns.
    AnswersAtOnce,
    /// Sends one item and ends the whole link in order.
    OneItemThenTheLinkCloses,
    /// Refuses the stream before the client's `send` returns.
    RefusesAtOnce,
}

/// One end of a link, with the other end played by [`Script`].
struct Scripted {
    script: Script,
    incoming: Mutex<VecDeque<Option<Vec<u8>>>>,
    arrived: Condvar,
}

impl Scripted {
    fn new(script: Script) -> Self {
        Self {
            script,
            incoming: Mutex::new(VecDeque::new()),
            arrived: Condvar::new(),
        }
    }

    fn deliver(&self, frames: Vec<Option<Vec<u8>>>) {
        self.incoming.lock().unwrap().extend(frames);
        self.arrived.notify_all();
        // Until the mux has taken them: what is scripted is the order, so it has to have happened.
        let until = Instant::now() + Duration::from_secs(10);
        while !self.incoming.lock().unwrap().is_empty() && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

impl Channel for Scripted {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        if let Some(Frame::Data { stream, .. }) = Frame::decode(message) {
            let data = |stream, result: serde_json::Value| {
                let answer = json!({ "jsonrpc": "2.0", "id": 1, "result": result });
                Some(
                    Frame::Data {
                        stream,
                        body: answer.to_string().into_bytes(),
                    }
                    .encode(),
                )
            };
            let close = |stream, reason, text: &str| {
                Some(
                    Frame::Close {
                        stream,
                        reason,
                        text: text.to_owned(),
                    }
                    .encode(),
                )
            };
            let task =
                json!({ "task": { "id": "t", "status": { "state": "TASK_STATE_COMPLETED" } } });
            match self.script {
                Script::AnswersAtOnce => self.deliver(vec![
                    data(stream, task),
                    Some(Frame::Finish { stream }.encode()),
                    close(stream, CloseReason::Done, ""),
                ]),
                Script::OneItemThenTheLinkCloses => {
                    self.deliver(vec![data(stream, task), None]);
                }
                Script::RefusesAtOnce => {
                    self.deliver(vec![close(stream, CloseReason::Refused, "no")]);
                }
            }
        }
        Ok(())
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        let wait = timeout.unwrap_or(Duration::from_secs(1));
        let mut incoming = self.incoming.lock().unwrap();
        if incoming.is_empty() {
            incoming = self.arrived.wait_timeout(incoming, wait).unwrap().0;
        }
        incoming.pop_front().ok_or(Failure::Timeout)
    }

    fn close(&self) {}
}

fn over(script: Script, body: impl FnOnce(&Client<'_, '_, Scripted>)) {
    let channel = Scripted::new(script);
    Mux::run(&channel, Side::Dialed, Limits::default(), None, |mux| {
        body(&Client::new(mux));
    });
}

fn request() -> SendMessageRequest {
    SendMessageRequest::new(Message::text(Role::User, "m", "x"))
}

#[test]
fn an_answer_that_beats_the_requests_finish_is_still_the_answer() {
    over(Script::AnswersAtOnce, |client| {
        let answer = client.send_message(&request());
        assert!(
            matches!(answer, Ok(SendMessageResponse::Task(_))),
            "{answer:?}"
        );
    });
    over(Script::AnswersAtOnce, |client| {
        let events: Result<Vec<_>, _> =
            client.send_streaming_message(&request()).unwrap().collect();
        assert_eq!(events.unwrap().len(), 1);
    });
}

#[test]
fn a_refusal_that_beats_the_requests_finish_is_still_a_refusal() {
    over(Script::RefusesAtOnce, |client| {
        let answer = client.send_message(&request());
        assert!(
            matches!(answer, Err(ClientError::NotGranted(_))),
            "{answer:?}"
        );
    });
}

#[test]
fn a_link_that_closes_in_order_mid_stream_does_not_finish_the_stream() {
    over(Script::OneItemThenTheLinkCloses, |client| {
        let mut events = client.send_streaming_message(&request()).unwrap();
        assert!(events.next().unwrap().is_ok());
        let end = events.next();
        assert!(
            matches!(end, Some(Err(ClientError::Interrupted))),
            "{end:?}"
        );
    });
}
