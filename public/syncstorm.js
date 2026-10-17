/**
 * syncstorm.js  –  SyncStorm client library  (Milestone 4)
 *
 * Changes from M3:
 *   • _peers map  – merges presence list + live cursor positions
 *   • onPresence  – now fires on cursor moves too (users have optional .cursor {x,y})
 *   • sendCursor  – actually wired (throttled to ~20 fps)
 *   • disconnect()– closes the socket WITHOUT setting _closed, so auto-reconnect
 *                   fires; useful for the "Go offline" demo button
 *
 * Public API
 * ----------
 *   const room = SyncStorm.join(roomId, { name })
 *   room.set(key, value)
 *   room.onState(state    => ...)   // full state object
 *   room.onPresence(users => ...)   // [{id, name, cursor:{x,y}|null}, ...]
 *   room.onStatus(status  => ...)   // 'connected' | 'reconnecting' | 'disconnected'
 *   room.sendCursor(x, y)           // throttled to ~20 fps
 *   room.disconnect()               // drop socket; library auto-reconnects
 *   room.leave()                    // permanently close; no reconnect
 *
 * State consistency
 * -----------------
 *   Server is source of truth. Each key has {value, rev}.
 *   Conflict policy: per-key last-write-wins in server-arrival order.
 *   On (re)connect: client REPLACES local state with snapshot, then replays
 *   offline queue. Stale local data never overwrites newer server data.
 *
 * Reconnect
 * ---------
 *   Exponential backoff: 1s → 2s → 4s → 8s → 10s (cap).
 */

(function (global) {
  'use strict';

  const BACKOFF_BASE      = 1000;
  const BACKOFF_MAX       = 10_000;
  const CURSOR_THROTTLE   = 50; // ms ≈ 20 fps

  // ── SyncStorm namespace ─────────────────────────────────────────────────────
  const SyncStorm = {
    join(roomId, options) {
      if (typeof roomId !== 'string' || roomId.trim() === '') {
        throw new Error('SyncStorm.join: roomId must be a non-empty string');
      }
      return new Room(roomId.trim(), options || {});
    },
  };

  // ── Room ────────────────────────────────────────────────────────────────────
  function Room(roomId, options) {
    this._roomId  = roomId;
    this._name    = (typeof options.name === 'string' && options.name.trim())
                      ? options.name.trim().slice(0, 32) : 'Anonymous';

    this._state   = {};    // key → value  (local mirror)
    this._queue   = [];    // [{key,value}] offline write queue
    this._peers   = {};    // clientId → {id, name, cursor}  (presence)

    this._ws         = null;
    this._status     = 'disconnected';
    this._retryDelay = BACKOFF_BASE;
    this._retryTimer = null;
    this._closed     = false;

    this._cbState    = null;
    this._cbPresence = null;
    this._cbStatus   = null;

    this._lastCursor = 0;  // timestamp of last cursor send

    this._connect();
  }

  // ── Public API ──────────────────────────────────────────────────────────────

  /**
   * Set a shared key.  While offline, writes are queued + applied optimistically.
   * On reconnect the queue is replayed after the server snapshot.
   */
  Room.prototype.set = function (key, value) {
    if (typeof key !== 'string' || key.trim() === '') {
      console.warn('[SyncStorm] set: key must be a non-empty string');
      return this;
    }
    key = key.trim();

    if (this._status === 'connected') {
      this._send({ type: 'set', key, value });
    } else {
      // Deduplicate per key (last write wins locally too)
      const i = this._queue.findIndex(q => q.key === key);
      if (i >= 0) this._queue[i] = { key, value };
      else        this._queue.push({ key, value });
      // Optimistic UI update
      this._state[key] = value;
      this._emitState();
    }
    return this;
  };

  /** Send cursor position. Automatically throttled. */
  Room.prototype.sendCursor = function (x, y) {
    const now = Date.now();
    if (now - this._lastCursor < CURSOR_THROTTLE) return this;
    this._lastCursor = now;
    if (this._status === 'connected') {
      this._send({ type: 'cursor', x, y });
    }
    return this;
  };

  /** Register state callback — receives shallow copy of full state. */
  Room.prototype.onState = function (cb) {
    this._cbState = cb;
    if (Object.keys(this._state).length > 0) cb(this._stateSnapshot());
    return this;
  };

  /**
   * Register presence callback.
   * Receives: Array<{ id:string, name:string, cursor:{x,y}|null }>
   * Fired on: join, leave, and cursor move.
   */
  Room.prototype.onPresence = function (cb) {
    this._cbPresence = cb;
    return this;
  };

  /** Register status callback — 'connected' | 'reconnecting' | 'disconnected'. */
  Room.prototype.onStatus = function (cb) {
    this._cbStatus = cb;
    cb(this._status);
    return this;
  };

  /**
   * Drop the current socket so the library auto-reconnects with backoff.
   * Useful for the "Go offline" demo button.
   * Queued offline writes survive and will be replayed after reconnect.
   */
  Room.prototype.disconnect = function () {
    if (this._ws) {
      // Null out the handler first so _scheduleReconnect fires correctly
      const ws = this._ws;
      this._ws = null;
      ws.onclose = null;
      ws.onerror = null;
      ws.close();
    }
    this._setStatus('reconnecting');
    this._scheduleReconnect();
    return this;
  };

  /** Permanently close. No more reconnect attempts. */
  Room.prototype.leave = function () {
    this._closed = true;
    clearTimeout(this._retryTimer);
    if (this._ws) {
      this._ws.onclose = null;
      this._ws.close();
      this._ws = null;
    }
    this._peers = {};
    this._setStatus('disconnected');
  };

  // ── Private ─────────────────────────────────────────────────────────────────

  Room.prototype._connect = function () {
    if (this._closed) return;

    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    let ws;
    try {
      ws = new WebSocket(`${proto}://${location.host}/ws`);
    } catch (e) {
      console.error('[SyncStorm] WebSocket error:', e);
      this._scheduleReconnect();
      return;
    }
    this._ws = ws;

    ws.onopen = () => {
      this._retryDelay = BACKOFF_BASE;
      this._send({ type: 'join', roomId: this._roomId, name: this._name });
    };

    ws.onmessage = (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      this._handleMessage(msg);
    };

    ws.onclose = () => {
      if (this._closed) return;
      this._ws = null;
      this._setStatus('reconnecting');
      this._scheduleReconnect();
    };

    ws.onerror = () => { /* onclose fires next */ };
  };

  Room.prototype._handleMessage = function (msg) {
    switch (msg.type) {

      case 'snapshot': {
        // Replace local state with authoritative snapshot
        const next = {};
        for (const k of Object.keys(msg.state)) next[k] = msg.state[k].value;
        this._state = next;

        // Clear peer cursors on reconnect (they'll come back via cursor messages)
        for (const id of Object.keys(this._peers)) {
          this._peers[id].cursor = null;
        }

        this._setStatus('connected');

        // Replay offline queue
        const queued = this._queue.splice(0);
        for (const item of queued) {
          this._send({ type: 'set', key: item.key, value: item.value });
        }

        this._emitState();
        break;
      }

      case 'update': {
        this._state[msg.key] = msg.value;
        this._emitState();
        break;
      }

      case 'presence': {
        // Rebuild _peers map; preserve existing cursor positions for users still present
        const next = {};
        for (const u of (msg.users || [])) {
          next[u.id] = {
            id:     u.id,
            name:   u.name,
            cursor: this._peers[u.id] ? this._peers[u.id].cursor : null,
          };
        }
        this._peers = next;
        this._emitPresence();
        break;
      }

      case 'cursor': {
        // A peer moved their cursor
        const peer = this._peers[msg.clientId];
        if (peer) {
          peer.cursor = { x: msg.x, y: msg.y };
        } else {
          // We might receive a cursor before presence — create a stub
          this._peers[msg.clientId] = {
            id:     msg.clientId,
            name:   msg.name || 'Unknown',
            cursor: { x: msg.x, y: msg.y },
          };
        }
        this._emitPresence();
        break;
      }

      case 'error': {
        console.warn('[SyncStorm] server error:', msg.message);
        break;
      }
    }
  };

  Room.prototype._scheduleReconnect = function () {
    if (this._closed) return;
    const delay = this._retryDelay;
    this._retryDelay = Math.min(this._retryDelay * 2, BACKOFF_MAX);
    console.log(`[SyncStorm] reconnecting in ${delay}ms…`);
    this._retryTimer = setTimeout(() => this._connect(), delay);
  };

  Room.prototype._setStatus = function (s) {
    if (this._status === s) return;
    this._status = s;
    if (this._cbStatus) this._cbStatus(s);
  };

  Room.prototype._emitState = function () {
    if (this._cbState) this._cbState(this._stateSnapshot());
  };

  Room.prototype._emitPresence = function () {
    if (this._cbPresence) this._cbPresence(Object.values(this._peers));
  };

  Room.prototype._stateSnapshot = function () {
    return Object.assign({}, this._state);
  };

  Room.prototype._send = function (obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  };

  // ── Export ──────────────────────────────────────────────────────────────────
  global.SyncStorm = SyncStorm;

}(typeof window !== 'undefined' ? window : global));
