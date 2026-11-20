// Minimal Action Cable client for manual testing (Node 22+, no dependencies).
//
//   node examples/ws-client.mjs [stream] [url]
//
// Connects with the `actioncable-v1-json` protocol, subscribes to a public stream
// (`$pubsub` channel; the server needs --public or --public_streams), and prints
// every message except pings. Set SHOW_PINGS=1 to print pings too.

const stream = process.argv[2] ?? "demo";
const url = process.argv[3] ?? "ws://localhost:8080/cable";
const identifier = JSON.stringify({ channel: "$pubsub", stream_name: stream });

const ws = new WebSocket(url, ["actioncable-v1-json"]);
const time = () => {
  const d = new Date();
  return `${d.toTimeString().slice(0, 8)}.${String(d.getMilliseconds()).padStart(3, "0")}`;
};
const log = (...args) => console.log(time(), ...args);

ws.addEventListener("open", () => log(`connected to ${url}`));
ws.addEventListener("message", (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === "ping" && !process.env.SHOW_PINGS) return;
  log("<-", event.data);
  if (msg.type === "welcome") {
    const subscribe = JSON.stringify({ command: "subscribe", identifier });
    log("->", subscribe);
    ws.send(subscribe);
  }
});
ws.addEventListener("close", (event) => {
  log(`closed (code ${event.code}${event.reason ? `, ${event.reason}` : ""})`);
  process.exit(0);
});
ws.addEventListener("error", () => log("connection error (is the server running?)"));
process.on("SIGINT", () => ws.close());
