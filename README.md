# SyncStorm

> **Multiplayer shared state in ~10 lines of JavaScript** — HACKODEC PS1 entry.

Drop one `<script>` tag into any web page and get rooms, shared state, live cursors,
presence, and automatic reconnect. No build step. No dependencies. Plain WebSocket.

---

## The ~10-line integration (tested)

```html
<!-- Step 1: one script tag -->
<script src="/syncstorm.js"></script>
```

```js
// Step 2: join a room
const room = SyncStorm.join('my-room', { name: 'Alice' });

// Step 3: react to changes
room.onState(state  => renderUI(state));
room.onPresence(users => showWhoIsOnline(users));
room.onStatus(s     => updateBadge(s));

// Step 4: write shared state (works offline too)
room.set('votes', { yes: 3, no: 1 });

// Step 5: live cursors (throttled automatically)
document.addEventListener('mousemove',
  e => room.sendCursor(e.clientX, e.clientY));
```

That's it. Everything else — reconnect, offline queue, snapshot, presence, cursors — is automatic.

---

## Quick start (local)

**Prerequisites:** Node.js ≥ 18

```bash
npm install
npm start
# → http://localhost:3000
```

| Page | URL |
|---|---|
| Landing | http://localhost:3000 |
| Live poll demo | http://localhost:3000/demo.html |
| API docs | http://localhost:3000/docs.html |

**Run the test suite** (server must be running):

```bash
npm test
# → 27 assertions, 0 failures
```

---

## Public API

```js
const room = SyncStorm.join(roomId, { name })  // join (or create) a room
room.set(key, value)          // write shared state; queued while offline
room.onState(state  => ...)   // fires on every state change (full state object)
room.onPresence(users => ...) // fires on join/leave/cursor move
room.onStatus(status => ...)  // 'connected' | 'reconnecting' | 'disconnected'
room.sendCursor(x, y)         // broadcast cursor position (~20 fps throttle)
room.disconnect()             // drop socket; auto-reconnect fires
room.leave()                  // permanently close; no reconnect
```

---

## State consistency

**The server is the source of truth.** Every key stores `{ value, rev }` where
`rev` is a strictly-increasing integer assigned in server-arrival order.

**Conflict policy — per-key Last-Write-Wins:**  
When two clients write the same key concurrently, the server applies both in the
order it receives them and increments `rev` each time. The last write to arrive
wins. All clients see the same update sequence and converge to the same value.

**Snapshot on (re)connect:**  
On every connect or reconnect the server sends a full snapshot. The client
*replaces* its local state — stale data never overwrites newer server data.

**Offline queue:**  
Writes made while offline are queued per key (last write per key deduplicates).
After the snapshot arrives, the queue is replayed. The server applies them with
the same LWW rule.

---

## Reconnect policy

Exponential backoff: **1 s → 2 s → 4 s → 8 s → 10 s (cap)**.  
Backoff resets to 1 s on every successful connection.

---

## Architecture

```
Browser (syncstorm.js)          Server (server.js)
──────────────────────          ──────────────────
SyncStorm.join()  ──── WS ────► handleJoin()
                                  • create room if new
                                  • send snapshot
                                  • broadcastPresence()
room.set()        ──── WS ────► handleSet()
                                  • rev++ (LWW)
                                  • broadcast update to room
room.sendCursor() ──── WS ────► handleCursor()
                                  • relay to other room members
```

- One HTTP + WebSocket server on one port — no CORS issues.
- Rooms are in-memory `Map` objects. State survives client churn but **not server restart**.
- Empty rooms are garbage-collected after 5 minutes.

---

## Known limitations

| Limitation | Detail |
|---|---|
| No persistence | Rooms are in memory. Server restart clears everything. |
| No auth | Anyone with a room ID can join. Use unguessable IDs. |
| No history | Late joiners get current state, not past events. |
| No CRDTs | Concurrent edits to the same key → LWW (last write wins). |
| Single process | No horizontal scaling without an external state store. |
| Cursor coords | Viewport-relative; assume identical layout across clients. |

---

## Deploy to Railway (recommended — no sleeping, WebSocket support)

Railway gives **\$5 free credit/month** with no cold-start sleeping.

### Step-by-step

1. **Install the Railway CLI** (or use the web dashboard):
   ```bash
   npm install -g @railway/cli
   railway login
   ```

2. **Initialise git** (if not done already):
   ```bash
   git init
   git add .
   git commit -m "M6: complete SyncStorm"
   ```

3. **Deploy:**
   ```bash
   railway init          # creates a new project
   railway up            # deploys current directory
   ```

4. **Set environment variable:**
   Railway auto-sets `PORT`. Your server already reads `process.env.PORT`.
   No extra env vars required.

5. **Get your URL:**
   ```bash
   railway domain        # e.g. https://syncstorm-production.up.railway.app
   ```

6. **Test with 3 tabs:**  
   Open `https://your-url.up.railway.app/demo.html` in 3 browser tabs.
   The WebSocket upgrades automatically to `wss://` (secure).

### Alternative: Render (free tier, may sleep after 15 min inactivity)

1. Push to GitHub.
2. Go to **https://render.com** → New → Web Service → connect your repo.
3. Build command: *(leave empty)*
4. Start command: `node server.js`
5. No secrets needed — `PORT` is set automatically.

> ⚠️ Render free tier spins down after 15 minutes of no HTTP traffic.
> A WebSocket connection alone does NOT prevent sleeping.
> Use Railway or Fly.io for a demo that stays awake.

### No secrets in the repo

The server reads only `PORT` from the environment. There are no API keys, database
credentials, or secrets. The `.gitignore` excludes `node_modules/`.

---

## File structure

```
syncstorm/
├── server.js          # Node.js + Express + ws — rooms, state, presence, cursors
├── test.js            # Automated test suite (27 assertions, 7 scenarios)
├── package.json
├── .gitignore
└── public/
    ├── syncstorm.js   # The client library (single file, no deps)
    ├── index.html     # Landing page
    ├── demo.html      # Live poll demo
    ├── docs.html      # API reference + consistency docs
    └── test.html      # M1 echo-bus test (kept for reference)
```

---

## Milestones completed

| # | Milestone | Status |
|---|---|---|
| 1 | Project setup + server; 2 tabs talk | ✅ |
| 2 | Rooms + shared state + snapshot on join | ✅ |
| 3 | Single-file library; demo uses only public API | ✅ |
| 4 | Cursors + presence + reconnect/recovery | ✅ |
| 5 | Automated test script: 7 scenarios, 27 assertions | ✅ |
| 6 | UI polish, docs, README, deployment | ✅ |

---

*MIT licence · HACKODEC PS1 · Rooms are in memory · No persistence across server restarts*
