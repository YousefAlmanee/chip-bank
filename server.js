/* ============================================================
   Chip Bank — multiplayer poker & blackjack chip tracker
   Zero-dependency Node server.
   - Serves the frontend from /public
   - Live state push via Server-Sent Events (SSE)
   - Authoritative per-room game state, in memory + best-effort
     file persistence so a restart won't wipe an active game.
   Run:  node server.js     (PORT env respected, default 3000)
   ============================================================ */
'use strict';

const http = require('http');
const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT      = process.env.PORT || 3000;
const PUBLIC    = path.join(__dirname, 'public');
const DATA_FILE = path.join(__dirname, 'data', 'rooms.json');
const ROOM_TTL  = 24 * 60 * 60 * 1000; // forget idle rooms after 24h

/* ---------------- room store ---------------- */
const rooms = new Map(); // code -> room

function newRoom(code, hostName, startChips) {
  return {
    code,
    startChips,
    mode: 'poker',
    hostId: null,
    players: [],     // {id, name, balance, token}
    pot: 0,
    bets: {},        // playerId -> chips in pot this hand
    folded: {},      // playerId -> true
    dealerId: null,
    bjBets: {},       // playerId -> current blackjack bet
    log: [],
    history: [],     // undo snapshots
    clients: new Set(), // live SSE responses (not persisted)
    updated: Date.now()
  };
}

function genCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no I,O,0,1
  let code;
  do {
    code = Array.from({ length: 4 }, () =>
      alphabet[crypto.randomInt(alphabet.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function genToken() { return crypto.randomBytes(16).toString('hex'); }
function genId()    { return 'p_' + crypto.randomBytes(6).toString('hex'); }

/* ---------------- persistence (best effort) ---------------- */
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(persist, 800);
}
function persist() {
  saveTimer = null;
  try {
    const out = {};
    for (const [code, r] of rooms) {
      out[code] = {
        code: r.code, startChips: r.startChips, mode: r.mode, hostId: r.hostId,
        players: r.players, pot: r.pot, bets: r.bets, folded: r.folded,
        dealerId: r.dealerId, bjBets: r.bjBets, log: r.log,
        history: r.history, updated: r.updated
      };
    }
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(out));
  } catch (e) { /* read-only fs etc. — fine, stay in memory */ }
}
function restore() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const data = JSON.parse(raw);
    for (const code in data) {
      const d = data[code];
      if (Date.now() - (d.updated || 0) > ROOM_TTL) continue;
      const r = newRoom(d.code, '', d.startChips);
      Object.assign(r, d);
      r.clients = new Set();
      rooms.set(code, r);
    }
    console.log(`Restored ${rooms.size} room(s).`);
  } catch (e) { /* no file yet */ }
}

/* ---------------- helpers ---------------- */
const fmt = n => Math.round(n);
function getP(room, id) { return room.players.find(p => p.id === id); }
function byToken(room, token) { return room.players.find(p => p.token === token); }

function addLog(room, text, kind) {
  room.log.unshift({ t: text, k: kind || '' });
  if (room.log.length > 250) room.log.pop();
}
function snapshot(room) {
  room.history.push(JSON.stringify({
    pot: room.pot, bets: room.bets, folded: room.folded, bjBets: room.bjBets,
    dealerId: room.dealerId, log: room.log,
    balances: room.players.map(p => ({ id: p.id, b: p.balance }))
  }));
  if (room.history.length > 80) room.history.shift();
}

/* state sent to clients (no tokens, no client handles) */
function publicState(room) {
  const connected = new Set([...room.clients].map(c => c._pid));
  return {
    code: room.code,
    startChips: room.startChips,
    mode: room.mode,
    hostId: room.hostId,
    pot: room.pot,
    dealerId: room.dealerId,
    canUndo: room.history.length > 0,
    players: room.players.map(p => ({
      id: p.id, name: p.name, balance: fmt(p.balance),
      inPot: room.bets[p.id] || 0,
      folded: !!room.folded[p.id],
      bjBet: room.bjBets[p.id] || 0,
      connected: connected.has(p.id)
    })),
    log: room.log.slice(0, 80)
  };
}

function broadcast(room) {
  room.updated = Date.now();
  const payload = `event: state\ndata: ${JSON.stringify(publicState(room))}\n\n`;
  for (const res of room.clients) {
    try { res.write(payload); } catch (e) { /* dropped */ }
  }
  scheduleSave();
}
function sendEnded(room) {
  const payload = `event: ended\ndata: {}\n\n`;
  for (const res of room.clients) { try { res.write(payload); } catch (e) {} }
}

/* ---------------- game actions ---------------- */
/* returns {ok:true} or {error:'...'} ; mutates room */
function applyAction(room, actor, action, payload = {}) {
  const isHost = actor.id === room.hostId;
  const targetId = payload.playerId || actor.id;
  const ownsTarget = isHost || targetId === actor.id;

  switch (action) {

    /* ---- poker: a player bets their own chips (host may bet for anyone) ---- */
    case 'bet': {
      if (!ownsTarget) return { error: 'You can only bet your own chips.' };
      const p = getP(room, targetId); if (!p) return { error: 'No such player.' };
      let amt = Math.floor(Number(payload.amount));
      if (!amt || amt < 1) return { error: 'Enter a valid amount.' };
      if (amt > p.balance) return { error: `${p.name} only has ${fmt(p.balance)}.` };
      snapshot(room);
      p.balance -= amt;
      room.bets[p.id] = (room.bets[p.id] || 0) + amt;
      room.pot += amt;
      delete room.folded[p.id];
      addLog(room, `${p.name} bet ${amt} into the pot`, 'dn');
      return { ok: true };
    }
    case 'allin': {
      if (!ownsTarget) return { error: 'Not your seat.' };
      const p = getP(room, targetId); if (!p) return { error: 'No such player.' };
      if (p.balance <= 0) return { error: 'No chips left.' };
      snapshot(room);
      const amt = p.balance;
      p.balance = 0;
      room.bets[p.id] = (room.bets[p.id] || 0) + amt;
      room.pot += amt;
      delete room.folded[p.id];
      addLog(room, `${p.name} is ALL IN for ${amt}`, 'dn');
      return { ok: true };
    }
    case 'fold': {
      if (!ownsTarget) return { error: 'Not your seat.' };
      room.folded[targetId] = true;
      return { ok: true };
    }
    case 'unfold': {
      if (!ownsTarget) return { error: 'Not your seat.' };
      delete room.folded[targetId];
      return { ok: true };
    }

    /* ---- poker: host distributes the pot ---- */
    case 'award': {
      if (!isHost) return { error: 'Only the host can award the pot.' };
      const winners = (payload.winners || []).filter(id => getP(room, id));
      if (!winners.length) return { error: 'Pick at least one winner.' };
      if (room.pot <= 0) return { error: 'Pot is empty.' };
      snapshot(room);
      const each = Math.floor(room.pot / winners.length);
      const rem = room.pot - each * winners.length;
      winners.forEach((id, i) => {
        const p = getP(room, id);
        const amt = each + (i === 0 ? rem : 0);
        p.balance += amt;
        addLog(room, `${p.name} won ${amt} from the pot`, 'up');
      });
      room.pot = 0; room.bets = {}; room.folded = {};
      return { ok: true };
    }
    case 'newhand': {
      if (!isHost) return { error: 'Only the host can start a new hand.' };
      snapshot(room);
      room.pot = 0; room.bets = {}; room.folded = {};
      addLog(room, 'New hand');
      return { ok: true };
    }

    /* ---- blackjack ---- */
    case 'setdealer': {
      if (!isHost) return { error: 'Only the host sets the dealer.' };
      if (!getP(room, payload.dealerId)) return { error: 'No such player.' };
      room.dealerId = payload.dealerId;
      addLog(room, `${getP(room, room.dealerId).name} is now the dealer`);
      return { ok: true };
    }
    case 'bjbet': {
      if (!ownsTarget) return { error: 'Not your seat.' };
      const p = getP(room, targetId); if (!p) return { error: 'No such player.' };
      if (p.id === room.dealerId) return { error: 'The dealer does not bet.' };
      let amt = Math.floor(Number(payload.amount));
      if (!amt || amt < 1) return { error: 'Enter a valid bet.' };
      if (amt > p.balance) return { error: `${p.name} only has ${fmt(p.balance)}.` };
      snapshot(room);
      p.balance -= amt;
      room.bjBets[p.id] = (room.bjBets[p.id] || 0) + amt;
      addLog(room, `${p.name} bet ${amt} at blackjack`, 'dn');
      return { ok: true };
    }
    case 'bjresolve': {
      if (!isHost) return { error: 'Only the dealer/host settles blackjack.' };
      const p = getP(room, payload.playerId); if (!p) return { error: 'No such player.' };
      const dealer = getP(room, room.dealerId); if (!dealer) return { error: 'Set a dealer first.' };
      const bet = room.bjBets[p.id] || 0;
      if (bet <= 0) return { error: 'No bet to settle.' };
      snapshot(room);
      if (payload.outcome === 'win') {
        p.balance += bet * 2; dealer.balance -= bet;
        addLog(room, `${p.name} won ${bet} vs dealer`, 'up');
      } else if (payload.outcome === 'bj') {
        const win = Math.floor(bet * 1.5);
        p.balance += bet + win; dealer.balance -= win;
        addLog(room, `${p.name} hit blackjack +${win}`, 'up');
      } else if (payload.outcome === 'push') {
        p.balance += bet;
        addLog(room, `${p.name} pushed (bet returned)`);
      } else { // lose
        dealer.balance += bet;
        addLog(room, `${p.name} lost ${bet} to dealer`, 'dn');
      }
      delete room.bjBets[p.id];
      return { ok: true };
    }

    /* ---- shared ---- */
    case 'setmode': {
      if (!isHost) return { error: 'Only the host switches the game.' };
      if (payload.mode !== 'poker' && payload.mode !== 'blackjack') return { error: 'Bad mode.' };
      room.mode = payload.mode;
      return { ok: true };
    }
    case 'undo': {
      if (!isHost) return { error: 'Only the host can undo.' };
      if (!room.history.length) return { error: 'Nothing to undo.' };
      const s = JSON.parse(room.history.pop());
      room.pot = s.pot; room.bets = s.bets; room.folded = s.folded;
      room.bjBets = s.bjBets; room.dealerId = s.dealerId; room.log = s.log;
      s.balances.forEach(({ id, b }) => { const p = getP(room, id); if (p) p.balance = b; });
      return { ok: true };
    }

    /* ---- host: manage players ---- */
    case 'manage': {
      if (!isHost) return { error: 'Host only.' };
      const p = getP(room, payload.playerId); if (!p) return { error: 'No such player.' };
      snapshot(room);
      if (typeof payload.name === 'string' && payload.name.trim()) p.name = payload.name.trim().slice(0, 24);
      if (payload.balance !== undefined && !isNaN(payload.balance)) p.balance = Math.floor(payload.balance);
      addLog(room, `Host updated ${p.name} (${fmt(p.balance)})`);
      return { ok: true };
    }
    case 'topup': {
      if (!isHost) return { error: 'Host only.' };
      const p = getP(room, payload.playerId); if (!p) return { error: 'No such player.' };
      snapshot(room);
      p.balance += room.startChips;
      addLog(room, `${p.name} rebought +${room.startChips}`, 'up');
      return { ok: true };
    }
    case 'removeplayer': {
      if (!isHost) return { error: 'Host only.' };
      if (payload.playerId === room.hostId) return { error: 'Cannot remove the host.' };
      const p = getP(room, payload.playerId); if (!p) return { error: 'No such player.' };
      room.players = room.players.filter(x => x.id !== p.id);
      delete room.bets[p.id]; delete room.folded[p.id]; delete room.bjBets[p.id];
      if (room.dealerId === p.id) room.dealerId = room.players[0] ? room.players[0].id : null;
      addLog(room, `${p.name} was removed`);
      return { ok: true };
    }

    case 'endgame': {
      if (!isHost) return { error: 'Only the host can end the game.' };
      return { ok: true, end: true };
    }

    default:
      return { error: 'Unknown action.' };
  }
}

/* ---------------- HTTP ---------------- */
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', c => { data += c; if (data.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch { resolve({}); } });
  });
}

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };

function serveStatic(req, res) {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(PUBLIC, path.normalize(urlPath).replace(/^(\.\.[/\\])+/, ''));
  if (!filePath.startsWith(PUBLIC)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(filePath, (err, content) => {
    if (err) {
      // SPA fallback to index.html
      fs.readFile(path.join(PUBLIC, 'index.html'), (e2, idx) => {
        if (e2) { res.writeHead(404); return res.end('Not found'); }
        res.writeHead(200, { 'Content-Type': MIME['.html'] }); res.end(idx);
      });
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(content);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const p = url.pathname;

  /* ---- SSE stream ---- */
  if (p === '/events' && req.method === 'GET') {
    const code = (url.searchParams.get('room') || '').toUpperCase();
    const token = url.searchParams.get('token') || '';
    const room = rooms.get(code);
    if (!room) { res.writeHead(404); return res.end('room gone'); }
    const player = byToken(room, token);
    if (!player) { res.writeHead(403); return res.end('bad token'); }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.write('retry: 3000\n\n');
    res._pid = player.id;
    room.clients.add(res);
    // send identity + immediate state
    res.write(`event: hello\ndata: ${JSON.stringify({ you: player.id, isHost: player.id === room.hostId })}\n\n`);
    res.write(`event: state\ndata: ${JSON.stringify(publicState(room))}\n\n`);
    broadcast(room); // tell others this player is now connected

    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
    req.on('close', () => {
      clearInterval(ping);
      room.clients.delete(res);
      if (rooms.has(room.code)) broadcast(room);
    });
    return;
  }

  /* ---- create room ---- */
  if (p === '/api/create' && req.method === 'POST') {
    const b = await readBody(req);
    const name = String(b.hostName || '').trim().slice(0, 24);
    const chips = Math.floor(Number(b.startChips));
    if (!name) return sendJSON(res, 400, { error: 'Enter your name.' });
    if (!chips || chips < 1) return sendJSON(res, 400, { error: 'Enter a valid starting amount.' });
    const code = genCode();
    const room = newRoom(code, name, chips);
    const host = { id: genId(), name, balance: chips, token: genToken() };
    room.players.push(host);
    room.hostId = host.id;
    room.dealerId = host.id;
    addLog(room, `Game created · ${chips} chips each`);
    rooms.set(code, room);
    scheduleSave();
    return sendJSON(res, 200, { roomCode: code, playerId: host.id, token: host.token, startChips: chips });
  }

  /* ---- join room ---- */
  if (p === '/api/join' && req.method === 'POST') {
    const b = await readBody(req);
    const code = String(b.roomCode || '').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return sendJSON(res, 404, { error: 'Room not found. Check the code.' });

    // reconnect with existing token
    if (b.token) {
      const existing = byToken(room, b.token);
      if (existing) return sendJSON(res, 200, { roomCode: code, playerId: existing.id, token: existing.token });
    }
    const name = String(b.name || '').trim().slice(0, 24);
    if (!name) return sendJSON(res, 400, { error: 'Enter your name.' });
    const player = { id: genId(), name, balance: room.startChips, token: genToken() };
    room.players.push(player);
    addLog(room, `${name} joined with ${room.startChips}`);
    if (!room.dealerId) room.dealerId = player.id;
    broadcast(room);
    return sendJSON(res, 200, { roomCode: code, playerId: player.id, token: player.token });
  }

  /* ---- action ---- */
  if (p === '/api/action' && req.method === 'POST') {
    const b = await readBody(req);
    const room = rooms.get(String(b.roomCode || '').toUpperCase());
    if (!room) return sendJSON(res, 404, { error: 'Room gone.' });
    const actor = byToken(room, b.token || '');
    if (!actor) return sendJSON(res, 403, { error: 'Not in this game.' });
    const result = applyAction(room, actor, b.action, b.payload || {});
    if (result.error) return sendJSON(res, 400, { error: result.error });
    if (result.end) {
      sendEnded(room);
      for (const c of room.clients) { try { c.end(); } catch {} }
      rooms.delete(room.code);
      scheduleSave();
      return sendJSON(res, 200, { ok: true, ended: true });
    }
    broadcast(room);
    return sendJSON(res, 200, { ok: true });
  }

  /* ---- health ---- */
  if (p === '/healthz') { return sendJSON(res, 200, { ok: true, rooms: rooms.size }); }

  /* ---- static ---- */
  return serveStatic(req, res);
});

/* periodic cleanup of stale rooms */
setInterval(() => {
  const now = Date.now();
  for (const [code, r] of rooms) {
    if (r.clients.size === 0 && now - r.updated > ROOM_TTL) rooms.delete(code);
  }
}, 60 * 60 * 1000);

restore();
server.listen(PORT, () => console.log(`Chip Bank running on http://localhost:${PORT}`));
