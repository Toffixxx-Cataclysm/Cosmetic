/**
 * Polaris Cosmetic Sync Server
 *
 * Peers are stored globally (by UUID + last known name) and also indexed into
 * rooms keyed by Minecraft server address. Cosmetics sync works even when two
 * clients compute different room keys, via periodic "sight" queries of nearby
 * players (match by UUID or name).
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

/** @typedef {{ uuid: string, name: string, cosmetics: number[], emote: number, updatedAt: number, server: string, ws: import("ws").WebSocket }} Peer */
/** @type {Map<string, Peer>} */
const peersByUuid = new Map();
/** @type {Map<string, string>} name(lower) -> uuid */
const uuidByName = new Map();
/** @type {Map<string, Set<string>>} serverKey -> set of uuids */
const rooms = new Map();

function now() {
  return Date.now();
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

function roomSet(serverKey) {
  const key = (serverKey || "unknown").toLowerCase();
  if (!rooms.has(key)) rooms.set(key, new Set());
  return rooms.get(key);
}

function indexName(peer) {
  const n = (peer.name || "").trim().toLowerCase();
  if (!n) return;
  const prev = uuidByName.get(n);
  if (prev && prev !== peer.uuid) {
    // Keep newest mapping.
  }
  uuidByName.set(n, peer.uuid);
}

function unindexName(peer) {
  const n = (peer.name || "").trim().toLowerCase();
  if (!n) return;
  if (uuidByName.get(n) === peer.uuid) uuidByName.delete(n);
}

function broadcastRoom(serverKey, exceptUuid, payload) {
  const set = rooms.get((serverKey || "").toLowerCase());
  if (!set) return;
  const data = JSON.stringify(payload);
  for (const uuid of set) {
    if (exceptUuid && uuid === exceptUuid) continue;
    const peer = peersByUuid.get(uuid);
    if (!peer || peer.ws.readyState !== 1) continue;
    peer.ws.send(data);
  }
}

function leave(ws) {
  const meta = ws.__polaris;
  if (!meta) return;
  const peer = peersByUuid.get(meta.uuid);
  if (!peer || peer.ws !== ws) {
    ws.__polaris = null;
    return;
  }
  peersByUuid.delete(meta.uuid);
  unindexName(peer);
  const set = rooms.get(meta.server);
  if (set) {
    set.delete(meta.uuid);
    if (set.size === 0) rooms.delete(meta.server);
  }
  broadcastRoom(meta.server, null, { type: "peer_leave", uuid: meta.uuid, name: peer.name });
  ws.__polaris = null;
}

function resolvePeerRef(ref) {
  if (!ref || typeof ref !== "object") return null;
  const uuid = String(ref.uuid || "").toLowerCase();
  const name = String(ref.name || "").trim().toLowerCase();
  if (uuid && peersByUuid.has(uuid)) return peersByUuid.get(uuid);
  if (name && uuidByName.has(name)) {
    const id = uuidByName.get(name);
    return peersByUuid.get(id) || null;
  }
  return null;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health" || url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(
      JSON.stringify({
        ok: true,
        service: "polaris-cosmetic-sync",
        version: "1.1.0",
        rooms: rooms.size,
        peers: peersByUuid.size,
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
      protocol: 2,
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

      if (ws.__polaris) leave(ws);

      const existing = peersByUuid.get(uuid);
      if (existing && existing.ws !== ws) {
        try {
          existing.ws.close(4002, "replaced");
        } catch {}
        // Clean previous room membership without broadcasting leave yet — replaced.
        const prevSet = rooms.get(existing.server);
        if (prevSet) {
          prevSet.delete(uuid);
          if (prevSet.size === 0) rooms.delete(existing.server);
        }
        unindexName(existing);
      }

      const peer = {
        uuid,
        name,
        cosmetics: sanitizeCosmetics(msg.cosmetics),
        emote: 0,
        updatedAt: now(),
        server: serverKey,
        ws,
      };
      peersByUuid.set(uuid, peer);
      indexName(peer);
      roomSet(serverKey).add(uuid);
      ws.__polaris = { uuid, server: serverKey };

      const roomPeers = [];
      for (const id of roomSet(serverKey)) {
        if (id === uuid) continue;
        const p = peersByUuid.get(id);
        if (p) roomPeers.push(publicPeer(p));
      }
      ws.send(JSON.stringify({ type: "snapshot", server: serverKey, peers: roomPeers }));
      broadcastRoom(serverKey, uuid, { type: "peer_join", peer: publicPeer(peer) });
      return;
    }

    const meta = ws.__polaris;
    if (!meta) {
      ws.send(JSON.stringify({ type: "error", error: "hello_required" }));
      return;
    }
    const peer = peersByUuid.get(meta.uuid);
    if (!peer || peer.ws !== ws) return;

    if (type === "state") {
      unindexName(peer);
      peer.name = String(msg.name || peer.name).slice(0, 32);
      peer.cosmetics = sanitizeCosmetics(msg.cosmetics);
      peer.updatedAt = now();
      indexName(peer);
      broadcastRoom(peer.server, peer.uuid, { type: "peer_update", peer: publicPeer(peer) });
      return;
    }

    if (type === "emote") {
      const emoteId = Math.trunc(Number(msg.emote) || 0);
      peer.emote = emoteId > 0 ? emoteId : 0;
      peer.updatedAt = now();
      broadcastRoom(peer.server, peer.uuid, {
        type: "peer_emote",
        uuid: peer.uuid,
        name: peer.name,
        emote: peer.emote,
        updatedAt: peer.updatedAt,
      });
      return;
    }

    // Nearby-player query: works across mismatched room keys / offline UUIDs.
    if (type === "sight") {
      const refs = Array.isArray(msg.players) ? msg.players : [];
      const found = [];
      const seen = new Set();
      for (const ref of refs.slice(0, 64)) {
        const p = resolvePeerRef(ref);
        if (!p || p.uuid === peer.uuid || seen.has(p.uuid)) continue;
        seen.add(p.uuid);
        found.push(publicPeer(p));
      }
      ws.send(JSON.stringify({ type: "sight_state", peers: found }));
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
