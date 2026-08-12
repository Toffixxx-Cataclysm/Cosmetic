/**
 * Polaris Cosmetic Sync — cataclysm2.0-style relay.
 *
 * Clients send JSON packets; server broadcasts them to every other socket.
 * Filtering by Minecraft serverId happens on the client.
 *
 * Env: PORT, HOST, SYNC_TOKEN (?token=)
 */

const http = require("http");
const { WebSocketServer } = require("ws");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const SYNC_TOKEN = process.env.SYNC_TOKEN || "";

function authorize(url) {
  if (!SYNC_TOKEN) return true;
  return (url.searchParams.get("token") || "") === SYNC_TOKEN;
}

function sendJson(res, code, obj) {
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (url.pathname === "/health" || url.pathname === "/") {
    sendJson(res, 200, {
      ok: true,
      service: "polaris-cosmetic-sync",
      version: "2.0.0",
      mode: "broadcast-relay",
      peers: wss ? wss.clients.size : 0,
      ws: "/ws",
    });
    return;
  }
  sendJson(res, 404, { ok: false, error: "not_found" });
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws, req) => {
  try {
    const url = new URL(req.url || "/ws", `http://${req.headers.host || "localhost"}`);
    if (!authorize(url)) {
      ws.close(4001, "unauthorized");
      return;
    }
  } catch {
    ws.close(4000, "bad_request");
    return;
  }

  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.send(JSON.stringify({ type: "hello_ok", protocol: 3, mode: "broadcast-relay" }));

  ws.on("message", (buf) => {
    const data = String(buf);
    // Fan-out to every other open client (same as cataclysm-emotions relay).
    for (const client of wss.clients) {
      if (client !== ws && client.readyState === 1) {
        try {
          client.send(data);
        } catch {}
      }
    }
  });
});

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      try {
        client.terminate();
      } catch {}
      continue;
    }
    client.isAlive = false;
    try {
      client.ping();
    } catch {}
  }
}, 25000);

wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, HOST, () => {
  console.log(`[PolarisSync] broadcast relay on http://${HOST}:${PORT}`);
  console.log(`[PolarisSync] websocket: /ws`);
});
