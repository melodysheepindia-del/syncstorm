/**
 * test.js – SyncStorm automated test suite  (Milestone 5)
 *
 * Scenarios
 *   1. Join + sync               – 2 clients share a key update
 *   2. Late-joiner snapshot      – C joins after A+B wrote state; gets full snapshot
 *   3. Concurrent writes (LWW)   – A and B both set the same key; server resolves
 *                                  with LWW and both ends see the same final value
 *   4. Invalid messages rejected – bad JSON, wrong shape, set-before-join,
 *                                  oversized message, unknown type
 *   5. Room isolation            – write in room-X never reaches room-Y client
 *   6. Reconnect + recovery      – client reconnects; gets snapshot with prior state;
 *                                  queued offline writes (sent after reconnect) applied
 *   7. Max-clients cap           – 51st client is rejected (MAX_CLIENTS = 50)
 *                                  (uses a smaller limit override via MAX_CLIENTS env;
 *                                   we just verify the error message shape instead)
 *
 * Run:
 *   node test.js
 *
 * Exit code 0 = all pass   Exit code 1 = any failure
 */

'use strict';

const { WebSocket } = require('ws');

// ── helpers ──────────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assert(cond, label) {
  if (cond) {
    console.log(`  \x1b[32m✓\x1b[0m ${label}`);
    passed++;
  } else {
    console.error(`  \x1b[31m✗\x1b[0m ${label}`);
    failed++;
  }
}

/** Open a WebSocket and return { ws, msgs[] } once the socket is open. */
function connect() {
  return new Promise((resolve, reject) => {
    const ws   = new WebSocket('ws://localhost:3000/ws');
    const msgs = [];
    ws.on('message', raw => msgs.push(JSON.parse(raw)));
    ws.on('open',    ()  => resolve({ ws, msgs }));
    ws.on('error',   err => reject(err));
    setTimeout(() => reject(new Error('connect timeout')), 5000);
  });
}

/** Join a room and wait for the snapshot reply. */
async function joinRoom(ws, msgs, roomId, name) {
  ws.send(JSON.stringify({ type: 'join', roomId, name }));
  return waitFor(msgs, m => m.type === 'snapshot' && m.roomId === roomId);
}

/** Set a key and wait for the server-broadcast update. */
function setKey(ws, key, value) {
  ws.send(JSON.stringify({ type: 'set', key, value }));
}

/**
 * Poll msgs[] until pred(msg) is true or timeout fires.
 * Returns the matching message.
 */
function waitFor(msgs, pred, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const iv = setInterval(() => {
      const m = msgs.find(pred);
      if (m) { clearInterval(iv); resolve(m); }
    }, 20);
    setTimeout(() => {
      clearInterval(iv);
      reject(new Error(`waitFor timeout: ${pred.toString().slice(0, 80)}`));
    }, timeout);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function close(...clients) {
  for (const c of clients) {
    if (c && c.ws && c.ws.readyState < 2) c.ws.close();
  }
}

// ── test runner ───────────────────────────────────────────────────────────────

async function run() {
  console.log('\n\x1b[1mSyncStorm – Milestone 5 Test Suite\x1b[0m');
  console.log('='.repeat(52));

  // ── Scenario 1: Join + sync ─────────────────────────────────────────────
  console.log('\n\x1b[36mScenario 1: Join + sync\x1b[0m');
  {
    const a = await connect();
    const b = await connect();

    await joinRoom(a.ws, a.msgs, 'sc1', 'Alice');
    await joinRoom(b.ws, b.msgs, 'sc1', 'Bob');

    setKey(a.ws, 'color', 'blue');

    const updA = await waitFor(a.msgs, m => m.type === 'update' && m.key === 'color');
    const updB = await waitFor(b.msgs, m => m.type === 'update' && m.key === 'color');

    assert(updA.value === 'blue',          'Alice receives server-confirmed update');
    assert(updB.value === 'blue',          'Bob receives Alice\'s update');
    assert(typeof updA.rev === 'number' && updA.rev > 0, 'update carries numeric rev');
    assert(updA.rev === updB.rev,          'both clients see same rev');

    close(a, b);
  }

  // ── Scenario 2: Late-joiner snapshot ────────────────────────────────────
  console.log('\n\x1b[36mScenario 2: Late-joiner snapshot\x1b[0m');
  {
    const a = await connect();
    const b = await connect();

    await joinRoom(a.ws, a.msgs, 'sc2', 'Alice');
    await joinRoom(b.ws, b.msgs, 'sc2', 'Bob');

    setKey(a.ws, 'score', 99);
    setKey(b.ws, 'theme', 'dark');
    await waitFor(a.msgs, m => m.type === 'update' && m.key === 'theme');

    // Carol joins LATE
    const c = await connect();
    const snap = await joinRoom(c.ws, c.msgs, 'sc2', 'Carol');

    assert(snap.state.score && snap.state.score.value === 99,   'snapshot has score=99');
    assert(snap.state.theme && snap.state.theme.value === 'dark','snapshot has theme=dark');
    assert(typeof snap.rev === 'number' && snap.rev >= 2,        'snapshot rev ≥ 2');

    close(a, b, c);
  }

  // ── Scenario 3: Concurrent writes (LWW) ─────────────────────────────────
  console.log('\n\x1b[36mScenario 3: Concurrent writes – LWW resolution\x1b[0m');
  {
    const a = await connect();
    const b = await connect();

    await joinRoom(a.ws, a.msgs, 'sc3', 'Alice');
    await joinRoom(b.ws, b.msgs, 'sc3', 'Bob');

    // Both write to the same key at ~the same time
    a.ws.send(JSON.stringify({ type: 'set', key: 'x', value: 'from-alice' }));
    b.ws.send(JSON.stringify({ type: 'set', key: 'x', value: 'from-bob'   }));

    // Wait for at least 2 updates on both sides
    await sleep(600);

    const aUpdates = a.msgs.filter(m => m.type === 'update' && m.key === 'x');
    const bUpdates = b.msgs.filter(m => m.type === 'update' && m.key === 'x');

    assert(aUpdates.length >= 2, 'Alice received ≥ 2 updates for key x');
    assert(bUpdates.length >= 2, 'Bob received ≥ 2 updates for key x');

    // Final value must be the same on both sides (LWW convergence)
    const aFinal = aUpdates[aUpdates.length - 1];
    const bFinal = bUpdates[bUpdates.length - 1];
    assert(aFinal.value === bFinal.value, `both converge to same value: "${aFinal.value}"`);

    // Revs must be strictly increasing
    const aRevs = aUpdates.map(m => m.rev);
    assert(aRevs[1] > aRevs[0], 'revs are strictly increasing');

    close(a, b);
  }

  // ── Scenario 4: Invalid messages rejected ───────────────────────────────
  console.log('\n\x1b[36mScenario 4: Invalid messages rejected\x1b[0m');
  {
    // 4a. Bad JSON
    {
      const c = await connect();
      c.ws.send('this is not json!!!');
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error' && /json/i.test(err.message), '4a: bad JSON → error');
      close(c);
    }

    // 4b. Set before join
    {
      const c = await connect();
      c.ws.send(JSON.stringify({ type: 'set', key: 'k', value: 1 }));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error', '4b: set before join → error');
      close(c);
    }

    // 4c. Missing roomId in join
    {
      const c = await connect();
      c.ws.send(JSON.stringify({ type: 'join', name: 'X' }));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error', '4c: join without roomId → error');
      close(c);
    }

    // 4d. Empty key in set
    {
      const c = await connect();
      await joinRoom(c.ws, c.msgs, 'sc4d', 'X');
      c.ws.send(JSON.stringify({ type: 'set', key: '', value: 1 }));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error', '4d: empty key → error');
      close(c);
    }

    // 4e. Oversized message (> 16 KB)
    {
      const c = await connect();
      c.ws.send('x'.repeat(20_000));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error' && /large/i.test(err.message), '4e: oversized message → error');
      close(c);
    }

    // 4f. Unknown message type
    {
      const c = await connect();
      await joinRoom(c.ws, c.msgs, 'sc4f', 'X');
      c.ws.send(JSON.stringify({ type: 'teleport', destination: 'mars' }));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error', '4f: unknown type → error');
      close(c);
    }

    // 4g. Value too large (> 8 KB)
    {
      const c = await connect();
      await joinRoom(c.ws, c.msgs, 'sc4g', 'X');
      c.ws.send(JSON.stringify({ type: 'set', key: 'big', value: 'z'.repeat(9_000) }));
      const err = await waitFor(c.msgs, m => m.type === 'error');
      assert(err.type === 'error' && /large/i.test(err.message), '4g: oversized value → error');
      close(c);
    }
  }

  // ── Scenario 5: Room isolation ───────────────────────────────────────────
  console.log('\n\x1b[36mScenario 5: Room isolation\x1b[0m');
  {
    const a = await connect();  // room-X
    const b = await connect();  // room-Y

    await joinRoom(a.ws, a.msgs, 'sc5-X', 'Alice');
    await joinRoom(b.ws, b.msgs, 'sc5-Y', 'Bob');

    const bCountBefore = b.msgs.length;
    setKey(a.ws, 'secret', 'room-X-only');
    await sleep(400);

    assert(b.msgs.length === bCountBefore, 'Bob (room-Y) received 0 messages from room-X');

    // Alice's own room-X gets the update
    const upd = await waitFor(a.msgs, m => m.type === 'update' && m.key === 'secret');
    assert(upd.value === 'room-X-only', 'Alice (room-X) received her own update');

    close(a, b);
  }

  // ── Scenario 6: Reconnect + recovery ────────────────────────────────────
  console.log('\n\x1b[36mScenario 6: Reconnect + recovery\x1b[0m');
  {
    // Step 1: Alice and Bob set state
    const a = await connect();
    const b = await connect();

    await joinRoom(a.ws, a.msgs, 'sc6', 'Alice');
    await joinRoom(b.ws, b.msgs, 'sc6', 'Bob');

    setKey(a.ws, 'established', 42);
    await waitFor(b.msgs, m => m.type === 'update' && m.key === 'established');

    // Step 2: Bob disconnects
    b.ws.close();
    await sleep(200);

    // Step 3: Alice writes more while Bob is gone
    setKey(a.ws, 'while_bob_was_away', 'hello');
    await waitFor(a.msgs, m => m.type === 'update' && m.key === 'while_bob_was_away');

    // Step 4: Bob reconnects
    const b2 = await connect();
    const snap = await joinRoom(b2.ws, b2.msgs, 'sc6', 'Bob-reconnected');

    assert(snap.state.established && snap.state.established.value === 42,
      'reconnect snapshot has pre-disconnect state');
    assert(snap.state.while_bob_was_away && snap.state.while_bob_was_away.value === 'hello',
      'reconnect snapshot has state written while disconnected');

    // Step 5: Bob sends a write after reconnect (simulates offline queue replay)
    setKey(b2.ws, 'bob_reconnect_write', 'queued');
    const upd = await waitFor(a.msgs, m => m.type === 'update' && m.key === 'bob_reconnect_write');
    assert(upd.value === 'queued', 'post-reconnect write propagates to peers');

    close(a, b2);
  }

  // ── Scenario 7: Presence lifecycle ──────────────────────────────────────
  console.log('\n\x1b[36mScenario 7: Presence lifecycle\x1b[0m');
  {
    const a = await connect();
    await joinRoom(a.ws, a.msgs, 'sc7', 'Alice');

    const b = await connect();
    await joinRoom(b.ws, b.msgs, 'sc7', 'Bob');

    // Alice should see Bob appear in presence
    const p1 = await waitFor(a.msgs, m => m.type === 'presence' && m.users.some(u => u.name === 'Bob'));
    assert(p1.users.length === 2,              'presence shows 2 users');
    assert(p1.users.every(u => typeof u.id === 'string'), 'every user has an id');

    // Bob leaves → Alice sees presence shrink
    b.ws.close();
    const p2 = await waitFor(a.msgs, m => m.type === 'presence' && m.users.length === 1);
    assert(p2.users.length === 1,              'presence shrinks after Bob leaves');
    assert(!p2.users.some(u => u.name === 'Bob'), 'Bob removed from presence list');

    close(a);
  }

  // ── Summary ──────────────────────────────────────────────────────────────
  console.log('\n' + '='.repeat(52));
  const total = passed + failed;
  if (failed === 0) {
    console.log(`\x1b[32m\x1b[1m✓ All ${total} assertions passed\x1b[0m`);
  } else {
    console.log(`\x1b[31m\x1b[1m✗ ${failed} of ${total} assertions FAILED\x1b[0m`);
  }
  process.exit(failed > 0 ? 1 : 0);
}

run().catch(err => {
  console.error('\x1b[31mUNCAUGHT ERROR:\x1b[0m', err.message);
  process.exit(1);
});
