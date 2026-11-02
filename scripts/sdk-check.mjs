// Runs an unmodified A2A SDK client through the loopback command to an agent on a real link.
//
//   COQUI_A2A_BIN=target/debug/coqui-a2a COQUI_A2A_ECHO=target/debug/examples/echo_server \
//   A2A_JS_SDK_DIR=/path/with/node_modules node scripts/sdk-check.mjs
//
// The SDK is @a2a-js/sdk (npm). Without it, or without the two programs, this prints why it
// skipped and exits 0: it is an optional check. Everything the SDK sends is printed, its token hidden.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";

const skip = (why) => {
  console.log(`skipped: ${why}`);
  process.exit(0);
};

const sdkDir = resolve(process.env.A2A_JS_SDK_DIR ?? ".");
const sdkEntry = join(sdkDir, "node_modules", "@a2a-js", "sdk", "dist", "client", "index.js");
if (!existsSync(sdkEntry)) skip(`@a2a-js/sdk is not installed under ${sdkDir} (set A2A_JS_SDK_DIR)`);
const loopbackBin = process.env.COQUI_A2A_BIN;
const echoBin = process.env.COQUI_A2A_ECHO;
if (!loopbackBin || !existsSync(loopbackBin)) skip("COQUI_A2A_BIN does not name the coqui-a2a binary");
if (!echoBin || !existsSync(echoBin)) skip("COQUI_A2A_ECHO does not name the echo_server example");

const { ClientFactory, ClientFactoryOptions, DefaultAgentCardResolver, JsonRpcTransportFactory } = await import(
  pathToFileURL(sdkEntry)
);
const sdkVersion = JSON.parse(
  readFileSync(join(sdkDir, "node_modules", "@a2a-js", "sdk", "package.json"), "utf8"),
).version;
console.log(`@a2a-js/sdk ${sdkVersion}`);

const children = [];
const dir = mkdtempSync(join(tmpdir(), "coqui-a2a-sdk-"));
const stop = () => {
  for (const child of children) child.kill();
  rmSync(dir, { recursive: true, force: true });
};
process.on("exit", stop);

// A line of a child's standard output, with a deadline.
const lines = (child) => {
  const reader = createInterface({ input: child.stdout });
  const queue = [];
  const waiting = [];
  reader.on("line", (line) => (waiting.length ? waiting.shift()(line) : queue.push(line)));
  return (ms = 20000) =>
    new Promise((done, fail) => {
      if (queue.length) return done(queue.shift());
      const timer = setTimeout(() => fail(new Error("no output in time")), ms);
      waiting.push((line) => {
        clearTimeout(timer);
        done(line);
      });
    });
};

const server = spawn(echoBin, ["--identity", join(dir, "server.pem"), "--allow-identity", join(dir, "client.pem")], {
  stdio: ["pipe", "pipe", "inherit"],
});
children.push(server);
const serverLine = lines(server);
const port = await serverLine();
const serverId = await serverLine();

const loopback = spawn(
  loopbackBin,
  ["loopback", "--url", `ws://127.0.0.1:${port}/link`, "--identity", join(dir, "client.pem"), "--peer", serverId],
  { stdio: ["ignore", "pipe", "inherit"], env: { ...process.env, COQUI_AGENTIC_PEER_HOME: dir } },
);
children.push(loopback);
const loopbackLine = lines(loopback);
const base = (await loopbackLine()).trim();
console.log(`loopback endpoint ${base}`);
// The command makes a token of its own, and says where it keeps it.
const authorization = `Bearer ${readFileSync((await loopbackLine()).trim(), "utf8").trim()}`;

// What the SDK sends, as seen from outside. Its token goes in here, as a client's own fetch would do it.
const sent = [];
const loggingFetch = async (input, init) => {
  const url = typeof input === "string" ? input : input.url;
  const headers = Object.fromEntries(new Headers(init?.headers ?? {}));
  sent.push({ method: init?.method ?? "GET", url, headers, body: init?.body });
  return fetch(input, { ...init, headers: { ...headers, authorization } });
};

// The endpoint listens before the link is up and answers 503 until it is, so wait for the card.
for (let tries = 0; ; tries += 1) {
  const probe = await fetch(`${base}.well-known/agent-card.json`, { headers: { authorization } }).catch(() => undefined);
  if (probe?.status === 200) break;
  if (tries > 100) throw new Error(`the endpoint never became ready (last status ${probe?.status})`);
  await new Promise((done) => setTimeout(done, 100));
}

const factory = new ClientFactory(
  ClientFactoryOptions.createFrom(ClientFactoryOptions.default, {
    transports: [new JsonRpcTransportFactory({ fetchImpl: loggingFetch })],
    cardResolver: new DefaultAgentCardResolver({ fetchImpl: loggingFetch }),
  }),
);
let failed = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${detail ? `: ${detail}` : ""}`);
  if (!ok) failed += 1;
};

try {
  const unauthorized = await fetch(`${base}.well-known/agent-card.json`);
  check("a client with no token is refused", unauthorized.status === 401, String(unauthorized.status));

  const client = await factory.createFromUrl(base);
  const card = await client.getAgentCard();
  check("the card was read and names the loopback endpoint", card.name === "Echo", JSON.stringify(card.supportedInterfaces));

  // The SDK's types follow the protocol buffers: the role is a number, a part holds a `content` case.
  const request = (id, text) => ({
    message: { messageId: id, role: 1, parts: [{ content: { $case: "text", value: text } }] },
  });
  const result = await client.sendMessage(request("sdk-1", "hello from the sdk"));
  const task = result.task ?? result;
  const state = String(task.status?.state);
  check("sendMessage completed a task", state.includes("COMPLETED") || state === "3", state);
  const text = JSON.stringify(task.artifacts ?? []);
  check("the task holds the echo", text.includes("echo: hello from the sdk"), text);

  const events = [];
  for await (const event of client.sendMessageStream(request("sdk-2", "streamed"))) {
    events.push(event);
  }
  check("sendMessageStream gave events and ended", events.length >= 3, `${events.length} events`);
  const lastText = JSON.stringify(events);
  check(
    "the stream carried the echo and ended in a completed task",
    lastText.includes("echo: streamed") && /"state":(3|"TASK_STATE_COMPLETED")/.test(lastText),
    lastText.slice(0, 400),
  );

  const again = await client.getTask({ id: task.id });
  check("getTask returned the task", again.id === task.id);
} catch (error) {
  failed += 1;
  console.log(`FAIL ${error?.stack ?? error}`);
}

console.log("--- what the SDK sent ---");
for (const request of sent) {
  console.log(JSON.stringify(request));
}
process.exitCode = failed ? 1 : 0;
stop();
process.exit(failed ? 1 : 0);
