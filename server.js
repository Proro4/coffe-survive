// Втеча на каву — простий сервер: віддає гру і пересилає стан гравців між браузерами.
// Уся симуляція (машини, ТЦК, боти) працює в браузері хоста матчу.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PER_ROOM = 16;
const MAX_PRESENCE = 12000;
const indexHtml = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/health')) { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
  res.end(indexHtml);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const rooms = new Map();

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const roomName = (url.searchParams.get('room') || 'main').slice(0, 32);
  let room = rooms.get(roomName);
  if (!room) { room = new Map(); rooms.set(roomName, room); }
  if (room.size >= MAX_PER_ROOM) { ws.close(1013, 'room full'); return; }

  const id = crypto.randomBytes(8).toString('hex');
  const me = { ws, presence: {} };
  const send = (c, msg) => { if (c.ws.readyState === 1) c.ws.send(msg); };
  const broadcast = obj => { const s = JSON.stringify(obj); for (const [pid, c] of room) if (pid !== id) send(c, s); };

  ws.send(JSON.stringify({ t: 'hello', you: id, peers: [...room].map(([peer, c]) => ({ peer, presence: c.presence })) }));
  room.set(id, me);
  broadcast({ t: 'upd', peer: id, presence: me.presence });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', data => {
    let m; try { m = JSON.parse(data); } catch { return; }
    if (!m || m.t !== 'p' || !m.patch || typeof m.patch !== 'object') return;
    for (const [k, v] of Object.entries(m.patch)) {
      if (!/^[a-z]{1,8}$/.test(k)) continue;
      if (v === null) delete me.presence[k]; else me.presence[k] = v;
    }
    if (JSON.stringify(me.presence).length > MAX_PRESENCE) me.presence = {};
    broadcast({ t: 'upd', peer: id, presence: me.presence });
  });
  ws.on('close', () => {
    room.delete(id);
    broadcast({ t: 'leave', peer: id });
    if (!room.size) rooms.delete(roomName);
  });
});

// тримаємо з'єднання живими і прибираємо «мертві»
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Втеча на каву: http://localhost:${PORT}`));
