/**
 * Polaris Cosmetic Sync Server
 *
 * Rooms are keyed by Minecraft server address (or "singleplayer").
 * Clients share equipped cosmetic IDs and live emote playback.
 *
 * Env:
 *   PORT          - HTTP/WS port (default 3000)
 *   HOST          - bind host (default 0.0.0.0)
 *   SYNC_TOKEN    - optional shared secret; clients send it as ?token=
 */

const http = require("http");
const { WebSocketServer } = require("ws");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const SYNC_TOKEN = process.env.SYNC_TOKEN || "";

/** @typedef {{ uuid: string, name: string, cosmetics: number[], emote: number, updatedAt: number, ws: import("ws").WebSocket }} Peer */
/** @type {Map<string, Map<string, Peer>>} */
const rooms = new Map();

function now() {
  return Date.now();
}

function roomOf(serverKey) {
  const key = (serverKey || "unknown").toLowerCase();
  if (!rooms.has(key)) rooms.set(key, new Map());
  return rooms.get(key);
}

function sanitizeCosmetics(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  const seen = new Set();
  for (const v of raw) {
    const id = Number(v);
    if (!Number.isFinite(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    out.push(Math.trunc(id));
    if (out.length >= 32) break;
  }
  return out;
}

function publicPeer(peer) {
  return {
    uuid: peer.uuid,
    name: peer.name,
    cosmetics: peer.cosmetics,
    emote: peer.emote,
    updatedAt: peer.updatedAt,
  };
}

function broadcast(room, exceptUuid, payload) {
  const data = JSON.stringify(payload);
  for (const peer of room.values()) {
    if (exceptUuid && peer.uuid === exceptUuid) continue;
    if (peer.ws.readyState === 1) peer.ws.send(data);
  }
}

function leave(ws) {
  const meta = ws.__polaris;
  if (!meta) return;
  const room = rooms.get(meta.server);
  if (!room) return;
  const peer = room.get(meta.uuid);
  if (!peer || peer.ws !== ws) return;
  room.delete(meta.uuid);
  broadcast(room, null, { type: "peer_leave", uuid: meta.uuid });
  if (room.size === 0) rooms.delete(meta.server);
  ws.__polaris = null;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health" || url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(
      JSON.stringify({
        ok: true,
        service: "polaris-cosmetic-sync",
        version: "1.0.0",
        rooms: rooms.size,
        peers: [...rooms.values()].reduce((n, r) => n + r.size, 0),
        ws: "/ws",
      })
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify({ ok: false, error: "not_found" }));
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws, req) => {
  try {
    const url = new URL(req.url || "/ws", `http://${req.headers.host || "localhost"}`);
    if (SYNC_TOKEN) {
      const token = url.searchParams.get("token") || "";
      if (token !== SYNC_TOKEN) {
        ws.close(4001, "unauthorized");
        return;
      }
    }
  } catch {
    ws.close(4000, "bad_request");
    return;
  }

  ws.__polaris = null;
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.send(
    JSON.stringify({
      type: "hello_ok",
      serverTime: now(),
      protocol: 1,
    })
  );

  ws.on("message", (buf) => {
    let msg;
    try {
      msg = JSON.parse(String(buf));
    } catch {
      return;
    }
    if (!msg || typeof msg !== "object") return;

    const type = String(msg.type || "");

    if (type === "hello") {
      const uuid = String(msg.uuid || "").toLowerCase();
      const name = String(msg.name || "Player").slice(0, 32);
      const serverKey = String(msg.server || "unknown").slice(0, 128).toLowerCase();
      if (!/^[0-9a-f-]{32,36}$/.test(uuid)) {
        ws.send(JSON.stringify({ type: "error", error: "bad_uuid" }));
        return;
      }

      // Replace previous socket for same uuid in room.
      if (ws.__polaris) leave(ws);

      const room = roomOf(serverKey);
      const existing = room.get(uuid);
      if (existing && existing.ws !== ws) {
        try {
          existing.ws.close(4002, "replaced");
        } catch {}
      }

      const peer = {
        uuid,
        name,
        cosmetics: sanitizeCosmetics(msg.cosmetics),
        emote: 0,
        updatedAt: now(),
        ws,
      };
      room.set(uuid, peer);
      ws.__polaris = { uuid, server: serverKey };

      ws.send(
        JSON.stringify({
          type: "snapshot",
          server: serverKey,
          peers: [...room.values()].filter((p) => p.uuid !== uuid).map(publicPeer),
        })
      );
      broadcast(room, uuid, { type: "peer_join", peer: publicPeer(peer) });
      return;
    }

    const meta = ws.__polaris;
    if (!meta) {
      ws.send(JSON.stringify({ type: "error", error: "hello_required" }));
      return;
    }
    const room = rooms.get(meta.server);
    const peer = room && room.get(meta.uuid);
    if (!peer) return;

    if (type === "state") {
      peer.name = String(msg.name || peer.name).slice(0, 32);
      peer.cosmetics = sanitizeCosmetics(msg.cosmetics);
      peer.updatedAt = now();
      broadcast(room, peer.uuid, { type: "peer_update", peer: publicPeer(peer) });
      return;
    }

    if (type === "emote") {
      const emoteId = Math.trunc(Number(msg.emote) || 0);
      peer.emote = emoteId > 0 ? emoteId : 0;
      peer.updatedAt = now();
      broadcast(room, peer.uuid, {
        type: "peer_emote",
        uuid: peer.uuid,
        name: peer.name,
        emote: peer.emote,
        updatedAt: peer.updatedAt,
      });
      return;
    }

    if (type === "ping") {
      ws.send(JSON.stringify({ type: "pong", t: msg.t || now() }));
    }
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
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
  console.log(`[PolarisSync] listening on http://${HOST}:${PORT}`);
  console.log(`[PolarisSync] websocket path: /ws`);
  if (SYNC_TOKEN) console.log(`[PolarisSync] token auth enabled`);
});