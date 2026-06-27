# ♠ Chip Bank — Multiplayer Poker & Blackjack

A live, shared **chip bank** for poker nights where you play with real cards but
have no physical chips. The host sets a starting stack, everyone joins the same
room from their own phone with a 4-letter code, and balances update in real time.

- **Poker** — players push their own chips into a shared pot; the host awards it to the winner(s) (splits supported).
- **Blackjack** — players bet against a chosen dealer; the host settles each hand (Blackjack pays 3:2).
- A **?** button in the corner explains the rules of both games and the app.

No accounts, no real money, no database to set up.

## Run locally

```bash
node server.js
# open http://localhost:3000
```

That's it — **zero dependencies**, just Node 18+. `npm start` works too.

To test multiplayer on one machine, open the page in two browser windows
(one creates the game, the other joins with the code).

To test from other phones on your home Wi-Fi, find your computer's local IP
(e.g. `192.168.1.x`) and have everyone open `http://192.168.1.x:3000`.

## Deploy (so everyone can join from anywhere)

The whole app is one Node process that serves the page **and** the live sync,
so any host that runs a Node web service works. It listens on `process.env.PORT`.

### Render (free, easiest)
1. Push this folder to a GitHub repo.
2. On [render.com](https://render.com) → **New → Web Service** → pick the repo.
3. Build command: *(leave blank)* · Start command: `node server.js`.
4. Deploy. Share the `https://your-app.onrender.com` URL — that's your table.

### Railway / Fly.io / Heroku-style
Same idea: Start command `node server.js`. No build step, no env vars required.

### A VPS
```bash
PORT=80 node server.js     # or put it behind nginx / caddy
```

> **SSE note:** the server already sends the headers needed to stream through
> proxies (`Cache-Control: no-transform`, `X-Accel-Buffering: no`) and pings
> every 25s to keep connections alive. If you front it with nginx, make sure
> proxy buffering is off for `/events`.

## How it works

- **Frontend** (`public/index.html`): one self-contained page. Renders from
  server state, sends actions via `POST /api/action`, receives live updates over
  a Server-Sent Events stream (`GET /events`).
- **Backend** (`server.js`): authoritative game state per room, held in memory
  and mirrored to `data/rooms.json` so a restart won't drop an active game.
  Idle rooms are forgotten after 24h.

### Roles & permissions
- **Host** = the banker: switches Poker/Blackjack, awards pots, settles
  blackjack, manages players (rename / fix balance / rebuy / remove), can undo,
  and can end the game.
- **Players** control only their own seat: bet, all-in, fold, place a blackjack bet.

This keeps payouts unambiguous while letting everyone manage their own chips.

## Files
```
server.js            # zero-dependency Node server (HTTP + SSE + game logic)
public/index.html    # the whole client UI
package.json         # start script + node engine
data/rooms.json      # auto-created; live room snapshots (safe to delete)
```
