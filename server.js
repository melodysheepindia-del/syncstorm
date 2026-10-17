/**
 * SyncStorm – server.js  (Milestone 3)
 *
 * Changes from M2:
 *   • broadcastPresence() – sent on join and leave
 *   • handleCursor()      – relay cursor position to other room members
 *   • 'cursor' added to message dispatch
 *
 * Everything from M2 is preserved (rooms, LWW state, snapshot, validation, GC).
 */

const express   = require('express');
const http      = require('http');
const { WebSocketServer } = require('ws');
const crypto    = require('crypto');
const path      = require('path');

// ─── configuration ────────────────────────────────────────────────────────────
const PORT          = process.env.PORT || 3000;
const MAX_MSG_BYTES = 16_384;      // 16 KB per message
const MAX_KEY_LEN   = 128;         // characters
const MAX_VAL_BYTES = 8_192;       // 8 KB per value
const MAX_CLIENTS   = 50;          // per room
const ROOM_TTL_MS   = 5 * 60_000; // 5 min – delete empty rooms

// ─── room store ───────────────────────────────────────────────────────────────
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id:      roomId,
      state:   new Map(),
      clients: new Set(),
      rev:     0,
      emptyAt: null,
    });
    console.log(`[ROOM] created  ${roomId}`);
  }
  return rooms.get(roomId);
}

function serializeState(room) {
  const out = {};
  for (const [k, v] of room.state) out[k] = v;
  return out;
}

// ─── empty-room GC ────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    if (room.clients.size === 0) {
      if (room.emptyAt === null) {
        room.emptyAt = now;
      } else if (now - room.emptyAt > ROOM_TTL_MS) {
        rooms.delete(id);
        console.log(`[ROOM] deleted  ${id} (empty for ${ROOM_TTL_MS / 1000}s)`);
      }
    } else {
      room.emptyAt = null;
    }
  }
}, 30_000);

// ─── HTTP server ──────────────────────────────────────────────────────────────
const app = express();
app.use(express.static(path.join(__dirname, 'public')));
const httpServer = http.createServer(app);

// ─── WebSocket server ─────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });

wss.on('connection', (ws) => {
  ws._clientId = crypto.randomUUID();
  ws._roomId   = null;
  ws._name     = 'Anonymous';
  ws._cursor   = null;  // { x, y }

  console.log(`[WS] connected  client=${ws._clientId}`);

  ws.on('message', (raw) => {
    // 1. size guard
    if (raw.length > MAX_MSG_BYTES) {
      return safeSend(ws, { type: 'error', message: `Message too large (max ${MAX_MSG_BYTES} bytes)` });
    }

    // 2. parse JSON
    let msg;
    try { msg = JSON.parse(raw); }
    catch {
      return safeSend(ws, { type: 'error', message: 'Invalid JSON' });
    }

    if (typeof msg !== 'object' || msg === null || typeof msg.type !== 'string') {
      return safeSend(ws, { type: 'error', message: 'Message must be a JSON object with a "type" string' });
    }

    // 3. dispatch
    switch (msg.type) {
      case 'join':   return handleJoin(ws, msg);
      case 'set':    return handleSet(ws, msg);
      case 'cursor': return handleCursor(ws, msg);
      default:
        safeSend(ws, { type: 'error', message: `Unknown message type: "${msg.type}"` });
    }
  });

  ws.on('close', () => {
    if (ws._roomId) leaveRoom(ws);
    console.log(`[WS] disconnected client=${ws._clientId}`);
  });

  ws.on('error', (err) => {
    console.error(`[WS] error client=${ws._clientId}:`, err.message);
    if (ws._roomId) leaveRoom(ws);
  });
});

// ─── handlers ─────────────────────────────────────────────────────────────────

function handleJoin(ws, msg) {
  if (typeof msg.roomId !== 'string' || msg.roomId.trim() === '') {
    return safeSend(ws, { type: 'error', message: '"roomId" must be a non-empty string' });
  }
  const roomId = msg.roomId.trim();

  // Leave previous room if switching rooms
  if (ws._roomId && ws._roomId !== roomId) leaveRoom(ws);

  const room = getOrCreateRoom(roomId);

  if (room.clients.size >= MAX_CLIENTS) {
    return safeSend(ws, { type: 'error', message: `Room is full (max ${MAX_CLIENTS} clients)` });
  }

  ws._roomId = roomId;
  ws._name   = (typeof msg.name === 'string' && msg.name.trim())
               ? msg.name.trim().slice(0, 32)
               : 'Anonymous';
  ws._cursor = null;

  room.clients.add(ws);
  room.emptyAt = null;

  console.log(`[ROOM] join  room=${roomId} client=${ws._clientId} name="${ws._name}"  peers=${room.clients.size}`);

  // Full snapshot → joining client
  safeSend(ws, {
    type:   'snapshot',
    roomId,
    state:  serializeState(room),
    rev:    room.rev,
  });

  // Updated presence → everyone in the room
  broadcastPresence(room);
}

function handleSet(ws, msg) {
  if (!ws._roomId) {
    return safeSend(ws, { type: 'error', message: 'Must join a room before sending "set"' });
  }

  if (typeof msg.key !== 'string' || msg.key.trim() === '') {
    return safeSend(ws, { type: 'error', message: '"key" must be a non-empty string' });
  }
  if (msg.key.length > MAX_KEY_LEN) {
    return safeSend(ws, { type: 'error', message: `"key" too long (max ${MAX_KEY_LEN} chars)` });
  }

  const valStr = JSON.stringify(msg.value);
  if (valStr.length > MAX_VAL_BYTES) {
    return safeSend(ws, { type: 'error', message: `"value" too large (max ${MAX_VAL_BYTES} bytes)` });
  }

  const room = rooms.get(ws._roomId);
  if (!room) {
    return safeSend(ws, { type: 'error', message: 'Room not found – please rejoin' });
  }

  // LWW: server-assigned rev, strictly increasing per room
  room.rev++;
  room.state.set(msg.key, { value: msg.value, rev: room.rev });

  console.log(`[STATE] set  room=${ws._roomId} key="${msg.key}" rev=${room.rev} by=${ws._clientId}`);

  const broadcast = { type: 'update', roomId: ws._roomId, key: msg.key, value: msg.value, rev: room.rev };
  for (const client of room.clients) safeSend(client, broadcast);
}

function handleCursor(ws, msg) {
  if (!ws._roomId) return;

  const x = typeof msg.x === 'number' ? msg.x : null;
  const y = typeof msg.y === 'number' ? msg.y : null;
  if (x === null || y === null) return;

  ws._cursor = { x, y };

  const room = rooms.get(ws._roomId);
  if (!room) return;

  // Relay to every OTHER client in the room
  const payload = JSON.stringify({
    type:     'cursor',
    clientId: ws._clientId,
    name:     ws._name,
    x, y,
  });
  for (const client of room.clients) {
    if (client !== ws && client.readyState === 1) client.send(payload);
  }
}

function leaveRoom(ws) {
  const roomId = ws._roomId;
  const room   = rooms.get(roomId);
  if (room) {
    room.clients.delete(ws);
    console.log(`[ROOM] leave room=${roomId} client=${ws._clientId}  peers=${room.clients.size}`);
    broadcastPresence(room);
  }
  ws._roomId = null;
}

// ─── presence broadcast ───────────────────────────────────────────────────────
function broadcastPresence(room) {
  const users   = Array.from(room.clients).map(c => ({ id: c._clientId, name: c._name }));
  const payload = JSON.stringify({ type: 'presence', users });
  for (const c of room.clients) {
    if (c.readyState === 1) c.send(payload);
  }
}

// ─── helpers ──────────────────────────────────────────────────────────────────
function safeSend(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// ─── start ────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`SyncStorm listening on http://localhost:${PORT}`);
});
