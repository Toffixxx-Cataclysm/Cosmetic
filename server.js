/**
 * Polaris Cosmetic Sync Server v1.2
 *
 * Transports:
 *   - WebSocket  /ws
 *   - HTTP JSON  /v1/presence  (reliable fallback for Minecraft clients)
 *
 * Peers are global (uuid + name). Sight queries match nearby players even when
 * Minecraft server-address room keys differ.
 */

const http = require("http");
const { WebSocketServer } = require("ws");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const SYNC_TOKEN = process.env.SYNC_TOKEN || "";
const PEER_TTL_MS = 45000;

/** @typedef {{ uuid: string, name: string, cosmetics: number[], emote: number, emoteAt: number, updatedAt: number, server: string, ws?: import("ws").WebSocket }} Peer */
/** @type {Map<string, Peer>} */
const peersByUuid = new Map();
/** @type {Map<string, string>} */
const uuidByName = new Map();
/** @type {Map<string, Set<string>>} */
const rooms = new Map();

function now() {
  return Date.now();
}

function authorize(reqUrl) {
  if (!SYNC_TOKEN) return true;
  const token = reqUrl.searchParams.get("token") || "";
  return token === SYNC_TOKEN;
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
    emoteAt: peer.emoteAt || 0,
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
  uuidByName.set(n, peer.uuid);
}

function unindexName(peer) {
  const n = (peer.name || "").trim().toLowerCase();
  if (!n) return;
  if (uuidByName.get(n) === peer.uuid) uuidByName.delete(n);
}

function pruneStale() {
  const t = now();
  for (const [uuid, peer] of peersByUuid) {
    if (peer.ws && peer.ws.readyState === 1) continue;
    if (t - peer.updatedAt > PEER_TTL_MS) {
      peersByUuid.delete(uuid);
      unindexName(peer);
      const set = rooms.get(peer.server);
      if (set) {
        set.delete(uuid);
        if (set.size === 0) rooms.delete(peer.server);
      }
    }
  }
}

function upsertPeer({ uuid, name, server, cosmetics, emote, keepEmote }) {
  const id = String(uuid || "").toLowerCase();
  if (!/^[0-9a-f-]{32,36}$/.test(id)) return null;
  const serverKey = String(server || "unknown").slice(0, 128).toLowerCase();
  const display = String(name || "Player").slice(0, 32);
  const prev = peersByUuid.get(id);
  if (prev) unindexName(prev);

  const peer = {
    uuid: id,
    name: display,
    cosmetics: cosmetics != null ? sanitizeCosmetics(cosmetics) : prev ? prev.cosmetics : [],
    emote: keepEmote && prev ? prev.emote : 0,
    emoteAt: keepEmote && prev ? prev.emoteAt : 0,
    updatedAt: now(),
    server: serverKey,
    ws: prev && prev.ws && prev.ws.readyState === 1 ? prev.ws : undefined,
  };

  if (emote != null) {
    const e = Math.trunc(Number(emote) || 0);
    peer.emote = e > 0 ? e : 0;
    peer.emoteAt = peer.emote > 0 ? now() : 0;
  }

  // Preserve active websocket from previous record if this upsert is HTTP.
  if (prev && prev.ws) peer.ws = prev.ws;

  if (prev && prev.server && prev.server !== serverKey) {
    const oldSet = rooms.get(prev.server);
    if (oldSet) {
      oldSet.delete(id);
      if (oldSet.size === 0) rooms.delete(prev.server);
    }
  }

  peersByUuid.set(id, peer);
  indexName(peer);
  roomSet(serverKey).add(id);
  return peer;
}

function resolvePeerRef(ref) {
  if (!ref || typeof ref !== "object") return null;
  const uuid = String(ref.uuid || "").toLowerCase();
  const name = String(ref.name || "").trim().toLowerCase();
  if (uuid && peersByUuid.has(uuid)) return peersByUuid.get(uuid);
  if (name && uuidByName.has(name)) {
    return peersByUuid.get(uuidByName.get(name)) || null;
  }
  return null;
}

function sightPeers(selfUuid, refs) {
  const found = [];
  const seen = new Set();
  for (const ref of (Array.isArray(refs) ? refs : []).slice(0, 64)) {
    const p = resolvePeerRef(ref);
    if (!p || p.uuid === selfUuid || seen.has(p.uuid)) continue;
    // Expire old one-shot emotes after 12s so they don't replay forever.
    if (p.emote > 0 && p.emoteAt && now() - p.emoteAt > 12000) {
      p.emote = 0;
    }
    seen.add(p.uuid);
    found.push(publicPeer(p));
  }
  return found;
}

function broadcastRoom(serverKey, exceptUuid, payload) {
  const set = rooms.get((serverKey || "").toLowerCase());
  if (!set) return;
  const data = JSON.stringify(payload);
  for (const uuid of set) {
    if (exceptUuid && uuid === exceptUuid) continue;
    const peer = peersByUuid.get(uuid);
    if (!peer || !peer.ws || peer.ws.readyState !== 1) continue;
    try {
      peer.ws.send(data);
    } catch {}
  }
}

function leaveWs(ws) {
  const meta = ws.__polaris;
  if (!meta) return;
  const peer = peersByUuid.get(meta.uuid);
  if (!peer || peer.ws !== ws) {
    ws.__polaris = null;
    return;
  }
  peer.ws = undefined;
  peer.updatedAt = now();
  // Keep HTTP presence alive briefly; don't hard-delete on WS close.
  broadcastRoom(meta.server, null, { type: "peer_leave", uuid: meta.uuid, name: peer.name });
  ws.__polaris = null;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > 1_000_000) {
        reject(new Error("too_large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "content-type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  });
  res.end(body);
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") {
      sendJson(res, 204, { ok: true });
      return;
    }

    if (url.pathname === "/health" || url.pathname === "/") {
      pruneStale();
      sendJson(res, 200, {
        ok: true,
        service: "polaris-cosmetic-sync",
        version: "1.2.0",
        rooms: rooms.size,
        peers: peersByUuid.size,
        ws: "/ws",
        http: "/v1/presence",
      });
      return;
    }

    if (url.pathname === "/v1/presence" && req.method === "POST") {
      if (!authorize(url)) {
        sendJson(res, 401, { ok: false, error: "unauthorized" });
        return;
      }
      pruneStale();
      const body = await readJson(req);
      const peer = upsertPeer({
        uuid: body.uuid,
        name: body.name,
        server: body.server || "unknown",
        cosmetics: body.cosmetics,
        emote: body.emote,
        keepEmote: body.emote == null,
      });
      if (!peer) {
        sendJson(res, 400, { ok: false, error: "bad_uuid" });
        return;
      }
      if (body.emote != null) {
        broadcastRoom(peer.server, peer.uuid, {
          type: "peer_emote",
          uuid: peer.uuid,
          name: peer.name,
          emote: peer.emote,
          updatedAt: peer.updatedAt,
        });
      } else {
        broadcastRoom(peer.server, peer.uuid, { type: "peer_update", peer: publicPeer(peer) });
      }
      const peers = sightPeers(peer.uuid, body.players || body.sight || []);
      sendJson(res, 200, { ok: true, peers, self: publicPeer(peer) });
      return;
    }

    sendJson(res, 404, { ok: false, error: "not_found" });
  } catch (e) {
    sendJson(res, 500, { ok: false, error: String(e && e.message ? e.message : e) });
  }
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

  ws.__polaris = null;
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.send(JSON.stringify({ type: "hello_ok", serverTime: now(), protocol: 2 }));

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
      const peer = upsertPeer({
        uuid: msg.uuid,
        name: msg.name,
        server: msg.server,
        cosmetics: msg.cosmetics,
        keepEmote: true,
      });
      if (!peer) {
        ws.send(JSON.stringify({ type: "error", error: "bad_uuid" }));
        return;
      }
      peer.ws = ws;
      ws.__polaris = { uuid: peer.uuid, server: peer.server };

      const roomPeers = [];
      for (const id of roomSet(peer.server)) {
        if (id === peer.uuid) continue;
        const p = peersByUuid.get(id);
        if (p) roomPeers.push(publicPeer(p));
      }
      ws.send(JSON.stringify({ type: "snapshot", server: peer.server, peers: roomPeers }));
      broadcastRoom(peer.server, peer.uuid, { type: "peer_join", peer: publicPeer(peer) });
      return;
    }

    const meta = ws.__polaris;
    if (!meta) {
      ws.send(JSON.stringify({ type: "error", error: "hello_required" }));
      return;
    }
    const peer = peersByUuid.get(meta.uuid);
    if (!peer) return;
    peer.ws = ws;
    peer.updatedAt = now();

    if (type === "state") {
      upsertPeer({
        uuid: peer.uuid,
        name: msg.name || peer.name,
        server: peer.server,
        cosmetics: msg.cosmetics,
        keepEmote: true,
      });
      const fresh = peersByUuid.get(peer.uuid);
      if (fresh) fresh.ws = ws;
      broadcastRoom(peer.server, peer.uuid, { type: "peer_update", peer: publicPeer(fresh || peer) });
      return;
    }

    if (type === "emote") {
      const fresh = upsertPeer({
        uuid: peer.uuid,
        name: peer.name,
        server: peer.server,
        cosmetics: peer.cosmetics,
        emote: msg.emote,
      });
      if (fresh) fresh.ws = ws;
      broadcastRoom(peer.server, peer.uuid, {
        type: "peer_emote",
        uuid: peer.uuid,
        name: peer.name,
        emote: fresh ? fresh.emote : 0,
        updatedAt: now(),
      });
      return;
    }

    if (type === "sight") {
      ws.send(JSON.stringify({ type: "sight_state", peers: sightPeers(peer.uuid, msg.players) }));
      return;
    }

    if (type === "ping") {
      ws.send(JSON.stringify({ type: "pong", t: msg.t || now() }));
    }
  });

  ws.on("close", () => leaveWs(ws));
  ws.on("error", () => leaveWs(ws));
});

const heartbeat = setInterval(() => {
  pruneStale();
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
  console.log(`[PolarisSync] websocket: /ws`);
  console.log(`[PolarisSync] http api:  /v1/presence`);
  if (SYNC_TOKEN) console.log(`[PolarisSync] token auth enabled`);
});
