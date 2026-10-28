const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN || ''; // пусто = гостевой режим для тестов

const path = require('path');
const fs = require('fs');

const app = express();
const publicDir = path.join(__dirname, 'public');
if (fs.existsSync(publicDir)) app.use(express.static(publicDir));

// Страница ищется в public/index.html, а если папки нет, то рядом с server.js
app.get('/', (req, res) => {
  const candidates = [path.join(publicDir, 'index.html'), path.join(__dirname, 'index.html')];
  const file = candidates.find((f) => fs.existsSync(f));
  if (!file) return res.status(500).send('index.html не найден. Положите его в папку public рядом с server.js');
  res.sendFile(file);
});

// ICE: STUN всегда, TURN если задан (нужен ~15% пользователей за строгим NAT)
let turnCache = { at: 0, list: [] };
async function getTurn() {
  // Вариант 1: Metered (METERED_APP + METERED_KEY), креды запрашиваются автоматически
  if (process.env.METERED_APP && process.env.METERED_KEY) {
    if (Date.now() - turnCache.at < 30 * 60 * 1000 && turnCache.list.length) return turnCache.list;
    try {
      const url = `https://${process.env.METERED_APP}.metered.live/api/v1/turn/credentials?apiKey=${process.env.METERED_KEY}`;
      const list = await (await fetch(url)).json();
      if (Array.isArray(list)) { turnCache = { at: Date.now(), list }; return list; }
      console.error('Metered: неожиданный ответ', list);
    } catch (e) { console.error('Metered error', e.message); }
    return turnCache.list;
  }
  // Вариант 2: свои статические данные
  if (process.env.TURN_URL) {
    return [{ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS }];
  }
  return [];
}

app.get('/config', async (req, res) => {
  const turn = await getTurn();
  res.json({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }, ...turn], hasTurn: turn.length > 0 });
});

// Проверка подписи Telegram initData -> id пользователя
function verifyTelegram(initData) {
  try {
    const p = new URLSearchParams(initData);
    const hash = p.get('hash');
    p.delete('hash');
    const str = [...p.entries()].map(([k, v]) => `${k}=${v}`).sort().join('\n');
    const key = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const h = crypto.createHmac('sha256', key).update(str).digest('hex');
    if (h !== hash) return null;
    return 'tg' + JSON.parse(p.get('user')).id;
  } catch {
    return null;
  }
}

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const queue = new Set();
const reports = new Map(); // id -> кол-во жалоб
const banned = new Set();
const GENDERS = ['m', 'f'];
const LOOKING = ['m', 'f', 'any'];

const send = (c, msg) => c.readyState === 1 && c.send(JSON.stringify(msg));

// Взаимная совместимость: я подхожу ему, а он мне
const compatible = (a, b) =>
  (a.looking === 'any' || a.looking === b.gender) &&
  (b.looking === 'any' || b.looking === a.gender);

function unlink(c, notify = true) {
  queue.delete(c);
  const p = c.peer;
  c.peer = null;
  if (p) {
    p.peer = null;
    if (notify) send(p, { t: 'left' });
  }
}

function enqueue(c) {
  for (const o of queue) {
    if (o !== c && o.uid !== c.uid && o.uid !== c.last && compatible(c, o)) {
      queue.delete(o);
      c.peer = o;
      o.peer = c;
      c.last = o.uid;
      o.last = c.uid;
      send(c, { t: 'match', initiator: true });
      send(o, { t: 'match', initiator: false });
      return;
    }
  }
  queue.add(c);
  send(c, { t: 'wait', online: wss.clients.size });
}

wss.on('connection', (ws) => {
  ws.uid = null;
  ws.peer = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === 'find') {
      const uid = BOT_TOKEN ? verifyTelegram(m.initData) : 'guest' + String(m.guest || '').slice(0, 32);
      if (!uid) return send(ws, { t: 'error', msg: 'Откройте приложение из Telegram' });
      if (banned.has(uid)) return send(ws, { t: 'error', msg: 'Доступ закрыт из-за жалоб' });
      if (!GENDERS.includes(m.gender) || !LOOKING.includes(m.looking)) return;
      ws.uid = uid;
      ws.gender = m.gender;
      ws.looking = m.looking;
      unlink(ws);
      enqueue(ws);
    } else if (m.t === 'signal' && ws.peer) {
      send(ws.peer, { t: 'signal', data: m.data });
    } else if (m.t === 'stop') {
      unlink(ws);
    } else if (m.t === 'report' && ws.peer) {
      const id = ws.peer.uid;
      const n = (reports.get(id) || 0) + 1;
      reports.set(id, n);
      if (n >= 3) banned.add(id);
      unlink(ws);
    }
  });

  ws.on('close', () => unlink(ws));
});

server.listen(PORT, () => console.log('Listening on ' + PORT));
