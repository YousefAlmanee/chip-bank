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

function newRoom(code, hostName, startChips, sb, bb) {
  return {
    code,
    startChips,
    sb: sb || 25,
    bb: bb || 50,
    mode: 'poker',
    hostId: null,
    players: [],     // {id, name, balance, token}
    pot: 0,           // poker: total committed this hand (display)
    buttonId: null,   // dealer button carries across hands
    hand: null,       // active poker hand state (see startHand)
    dealerId: null,   // blackjack dealer
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
        code: r.code, startChips: r.startChips, sb: r.sb, bb: r.bb,
        mode: r.mode, hostId: r.hostId, players: r.players, pot: r.pot,
        buttonId: r.buttonId, hand: r.hand,
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
    pot: room.pot, buttonId: room.buttonId, hand: room.hand,
    bjBets: room.bjBets, dealerId: room.dealerId, log: room.log,
    balances: room.players.map(p => ({ id: p.id, b: p.balance }))
  }));
  if (room.history.length > 80) room.history.shift();
}

/* state sent to clients (no tokens, no client handles) */
function publicState(room) {
  const connected = new Set([...room.clients].map(c => c._pid));
  const h = room.hand;
  let pots = null;
  if (h && h.bettingDone) {
    pots = h.pots.map(pot => ({
      amount: pot.amount,
      eligible: pot.eligible.map(id => { const pl = getP(room, id); return pl ? pl.name : '?'; }),
      eligibleIds: pot.eligible
    }));
  }
  return {
    code: room.code,
    startChips: room.startChips,
    sb: room.sb, bb: room.bb,
    mode: room.mode,
    hostId: room.hostId,
    pot: room.pot,
    dealerId: room.dealerId,
    canUndo: room.history.length > 0,
    hand: h ? {
      active: h.active, bettingDone: !!h.bettingDone, street: h.street,
      message: h.message, pot: h.pot, currentBet: h.currentBet,
      sb: h.sb, bb: h.bb, buttonId: h.buttonId, sbId: h.sbId, bbId: h.bbId,
      toActId: h.toActId, pots
    } : null,
    players: room.players.map(p => {
      const seated = !!(h && h.order.includes(p.id));
      return {
        id: p.id, name: p.name, balance: fmt(p.balance),
        connected: connected.has(p.id),
        // poker per-hand
        seated,
        inHand: !!(h && h.inHand[p.id]),
        folded: !!(h && seated && !h.inHand[p.id]),
        allIn: !!(h && h.allIn[p.id]),
        streetBet: (h && h.streetBet[p.id]) || 0,
        committed: (h && h.contrib[p.id]) || 0,
        isButton: !!(h && h.buttonId === p.id),
        isSB: !!(h && h.sbId === p.id),
        isBB: !!(h && h.bbId === p.id),
        isToAct: !!(h && h.toActId === p.id),
        // blackjack
        bjBet: room.bjBets[p.id] || 0
      };
    }),
    log: room.log.slice(0, 80)
  };
}

/* ============================================================
   POKER ENGINE — round-based no-limit Hold'em (casual raises)
   ============================================================ */
function pokerContribute(room, pid, amount) {
  const h = room.hand, p = getP(room, pid);
  const pay = Math.min(amount, p.balance);
  p.balance -= pay;
  h.streetBet[pid] = (h.streetBet[pid] || 0) + pay;
  h.contrib[pid] = (h.contrib[pid] || 0) + pay;
  h.pot += pay;
  room.pot = h.pot;
  if (p.balance <= 0) h.allIn[pid] = true;
  return pay;
}
function nextInHand(room, fromId) {
  const h = room.hand, n = h.order.length;
  const start = h.order.indexOf(fromId);
  for (let i = 1; i <= n; i++) {
    const id = h.order[(start + i) % n];
    if (h.inHand[id] && !h.allIn[id]) return id;
  }
  return null;
}
function firstAfterButton(room) {
  // first player still in hand & able to act, starting left of the button
  const h = room.hand, n = h.order.length;
  const bIdx = h.order.indexOf(h.buttonId);
  for (let i = 1; i <= n; i++) {
    const id = h.order[(bIdx + i) % n];
    if (h.inHand[id] && !h.allIn[id]) return id;
  }
  return null;
}
function roundComplete(room) {
  const h = room.hand;
  const contenders = h.order.filter(id => h.inHand[id] && !h.allIn[id]);
  if (contenders.length === 0) return true;
  return contenders.every(id => h.acted[id] && h.streetBet[id] === h.currentBet);
}
function aliveCount(room) {
  const h = room.hand;
  return h.order.filter(id => h.inHand[id]).length;
}
function startHand(room) {
  const seated = room.players.filter(p => p.balance > 0);
  if (seated.length < 2) return { error: 'Need at least 2 players with chips.' };
  snapshot(room);
  const order = seated.map(p => p.id);
  // rotate button to next seated player after the previous button
  let buttonId;
  if (room.buttonId && order.includes(room.buttonId)) {
    const idx = order.indexOf(room.buttonId);
    buttonId = order[(idx + 1) % order.length];
  } else if (room.buttonId) {
    // previous button not seated this hand — pick the next seated after their old slot
    buttonId = order[0];
  } else {
    buttonId = order[0];
  }
  room.buttonId = buttonId;

  const h = {
    active: true, bettingDone: false, street: 'preflop',
    sb: room.sb, bb: room.bb,
    order, buttonId,
    inHand: {}, allIn: {}, contrib: {}, streetBet: {}, acted: {},
    currentBet: 0, lastAggressorId: null, toActId: null,
    pot: 0, pots: [], message: '', result: ''
  };
  order.forEach(id => { h.inHand[id] = true; });
  room.hand = h;
  room.pot = 0;

  const bIdx = order.indexOf(buttonId);
  let sbId, bbId;
  if (order.length === 2) {            // heads-up: button is SB
    sbId = buttonId;
    bbId = order[(bIdx + 1) % 2];
  } else {
    sbId = order[(bIdx + 1) % order.length];
    bbId = order[(bIdx + 2) % order.length];
  }
  h.sbId = sbId; h.bbId = bbId;
  pokerContribute(room, sbId, h.sb);
  pokerContribute(room, bbId, h.bb);
  h.currentBet = Math.max(h.streetBet[sbId], h.streetBet[bbId]);
  // blinds are forced; posters still get to act (BB option), so leave acted=false
  // first to act preflop: heads-up = button(SB); else left of BB
  h.toActId = order.length === 2 ? buttonId : nextInHand(room, bbId);
  addLog(room, `New hand · ${getP(room, buttonId).name} on the button · blinds ${h.sb}/${h.bb}`);
  return { ok: true };
}
function advanceStreet(room) {
  const h = room.hand;
  const streets = ['preflop', 'flop', 'turn', 'river'];
  const idx = streets.indexOf(h.street);
  if (idx >= streets.length - 1) return showdown(room);
  // need at least 2 players who can still bet to keep betting
  const contenders = h.order.filter(id => h.inHand[id] && !h.allIn[id]);
  if (contenders.length < 2) return showdown(room); // run remaining cards out
  h.street = streets[idx + 1];
  h.order.forEach(id => { h.streetBet[id] = 0; });
  h.currentBet = 0; h.acted = {}; h.lastAggressorId = null;
  h.toActId = firstAfterButton(room);
  const names = { flop: 'the flop (3 cards)', turn: 'the turn (1 card)', river: 'the river (1 card)' };
  h.message = 'Deal ' + names[h.street];
  return { ok: true };
}
function showdown(room) {
  const h = room.hand;
  h.bettingDone = true;
  h.toActId = null;
  h.street = 'showdown';
  h.pots = buildPots(room);
  // auto-award any pot with a single eligible player
  h.pots.forEach(pot => {
    if (pot.eligible.length === 1) {
      const p = getP(room, pot.eligible[0]);
      addLog(room, `${p.name} won ${pot.amount} at showdown`, 'up');
      p.balance += pot.amount; pot.awarded = true;
    }
  });
  h.pots = h.pots.filter(pot => !pot.awarded);
  h.message = h.pots.length ? 'Showdown — host awards the pot' : 'Hand complete';
  if (h.pots.length === 0) endHand(room);
  return { ok: true };
}
function buildPots(room) {
  const h = room.hand;
  const contribs = h.order.map(id => ({ id, amt: h.contrib[id] || 0 })).filter(x => x.amt > 0);
  const levels = [...new Set(contribs.map(c => c.amt))].sort((a, b) => a - b);
  const pots = [];
  let prev = 0;
  for (const lvl of levels) {
    const slice = lvl - prev;
    const contributors = contribs.filter(c => c.amt >= lvl);
    const amount = slice * contributors.length;
    const eligible = contributors.filter(c => h.inHand[c.id]).map(c => c.id);
    if (amount > 0 && eligible.length) pots.push({ amount, eligible });
    else if (amount > 0 && !eligible.length) { // all folded at this level — give to last main pot
      if (pots.length) pots[pots.length - 1].amount += amount;
    }
    prev = lvl;
  }
  // merge consecutive pots with identical eligible sets
  const merged = [];
  for (const pot of pots) {
    const last = merged[merged.length - 1];
    if (last && last.eligible.length === pot.eligible.length &&
        last.eligible.every(x => pot.eligible.includes(x))) last.amount += pot.amount;
    else merged.push({ amount: pot.amount, eligible: pot.eligible.slice() });
  }
  return merged;
}
function checkAutoWin(room) {
  const h = room.hand;
  if (aliveCount(room) === 1) {
    const id = h.order.find(x => h.inHand[x]);
    const p = getP(room, id);
    p.balance += h.pot;
    addLog(room, `${p.name} won ${h.pot} (everyone else folded)`, 'up');
    h.result = `${p.name} won ${h.pot}`;
    endHand(room);
    return true;
  }
  return false;
}
function endHand(room) {
  const h = room.hand;
  h.active = false; h.bettingDone = false; h.toActId = null;
  h.street = 'done';
  room.pot = 0;
}
function pokerMove(room, actor, payload) {
  const h = room.hand;
  if (!h || !h.active) return { error: 'No hand in progress.' };
  const pid = h.toActId;
  if (!pid) return { error: 'Not a betting moment.' };
  const isHost = actor.id === room.hostId;
  if (actor.id !== pid && !isHost) return { error: "It's not your turn." };
  const p = getP(room, pid);
  const move = payload.move;
  const toCall = h.currentBet - (h.streetBet[pid] || 0);

  snapshot(room);
  if (move === 'fold') {
    h.inHand[pid] = false; h.acted[pid] = true;
    addLog(room, `${p.name} folds`);
    if (checkAutoWin(room)) return { ok: true };
  } else if (move === 'check') {
    if (toCall > 0) { room.history.pop(); return { error: `You must call ${toCall} or fold.` }; }
    h.acted[pid] = true;
    addLog(room, `${p.name} checks`);
  } else if (move === 'call') {
    if (toCall <= 0) { room.history.pop(); return { error: 'Nothing to call — check instead.' }; }
    const paid = pokerContribute(room, pid, toCall);
    h.acted[pid] = true;
    addLog(room, `${p.name} calls ${paid}${h.allIn[pid] ? ' (all in)' : ''}`, 'dn');
  } else if (move === 'raise') {
    const target = Math.floor(Number(payload.amount)); // raise-TO (total this street)
    const maxTo = (h.streetBet[pid] || 0) + p.balance;
    if (isNaN(target) || target <= h.currentBet) { room.history.pop(); return { error: `Raise must be more than ${h.currentBet}.` }; }
    if (target > maxTo) { room.history.pop(); return { error: `${p.name} can only make it ${maxTo}.` }; }
    const pay = target - (h.streetBet[pid] || 0);
    pokerContribute(room, pid, pay);
    h.currentBet = h.streetBet[pid];
    h.lastAggressorId = pid;
    h.order.forEach(id => { h.acted[id] = false; });
    h.acted[pid] = true;
    addLog(room, `${p.name} raises to ${h.currentBet}${h.allIn[pid] ? ' (all in)' : ''}`, 'dn');
  } else if (move === 'allin') {
    if (p.balance <= 0) { room.history.pop(); return { error: 'No chips left.' }; }
    const before = h.currentBet;
    const paid = pokerContribute(room, pid, p.balance);
    h.acted[pid] = true;
    const myBet = h.streetBet[pid];
    if (myBet > before) {           // counts as a raise
      h.currentBet = myBet; h.lastAggressorId = pid;
      h.order.forEach(id => { if (id !== pid) h.acted[id] = false; });
      h.acted[pid] = true;
    }
    addLog(room, `${p.name} is ALL IN for ${paid}`, 'dn');
  } else {
    room.history.pop();
    return { error: 'Unknown move.' };
  }

  if (h.active) {
    if (roundComplete(room)) advanceStreet(room);
    else h.toActId = nextInHand(room, pid);
  }
  return { ok: true };
}
function awardShowdown(room, payload) {
  const h = room.hand;
  if (!h || !h.bettingDone) return { error: 'No pot to award yet.' };
  const awards = payload.awards || []; // [{potIndex, winners:[ids]}]
  // validate every remaining pot has winners
  for (let i = 0; i < h.pots.length; i++) {
    const a = awards.find(x => x.potIndex === i);
    if (!a || !a.winners || !a.winners.length) return { error: 'Pick a winner for every pot.' };
    if (!a.winners.every(w => h.pots[i].eligible.includes(w))) return { error: 'Winner not eligible for that pot.' };
  }
  snapshot(room);
  h.pots.forEach((pot, i) => {
    const winners = awards.find(x => x.potIndex === i).winners;
    const each = Math.floor(pot.amount / winners.length);
    const rem = pot.amount - each * winners.length;
    winners.forEach((w, j) => {
      const p = getP(room, w);
      const amt = each + (j === 0 ? rem : 0);
      p.balance += amt;
      addLog(room, `${p.name} won ${amt}${h.pots.length > 1 ? ' (pot ' + (i + 1) + ')' : ''}`, 'up');
    });
  });
  endHand(room);
  return { ok: true };
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

    /* ---- poker betting engine ---- */
    case 'starthand': {
      if (!isHost) return { error: 'Only the host deals the next hand.' };
      if (room.hand && room.hand.active) return { error: 'A hand is already in progress.' };
      return startHand(room);
    }
    case 'pokermove': {
      return pokerMove(room, actor, payload);
    }
    case 'awardshowdown': {
      if (!isHost) return { error: 'Only the host awards the pot.' };
      return awardShowdown(room, payload);
    }
    case 'setblinds': {
      if (!isHost) return { error: 'Only the host sets blinds.' };
      if (room.hand && room.hand.active) return { error: 'Finish the hand first.' };
      const sb = Math.floor(Number(payload.sb)), bb = Math.floor(Number(payload.bb));
      if (!sb || sb < 1 || !bb || bb < 1) return { error: 'Enter valid blinds.' };
      if (bb < sb) return { error: 'Big blind must be ≥ small blind.' };
      room.sb = sb; room.bb = bb;
      addLog(room, `Blinds set to ${sb}/${bb}`);
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
      room.pot = s.pot; room.buttonId = s.buttonId; room.hand = s.hand;
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
      if (room.hand && room.hand.active && room.hand.inHand[p.id]) return { error: 'Cannot remove a player mid-hand. Finish the hand first.' };
      room.players = room.players.filter(x => x.id !== p.id);
      delete room.bjBets[p.id];
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
    const sb = Math.floor(Number(b.sb)) || 25;
    const bb = Math.floor(Number(b.bb)) || 50;
    if (!name) return sendJSON(res, 400, { error: 'Enter your name.' });
    if (!chips || chips < 1) return sendJSON(res, 400, { error: 'Enter a valid starting amount.' });
    const code = genCode();
    const room = newRoom(code, name, chips, sb, bb);
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
