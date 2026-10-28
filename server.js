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

// ---- Хранилище в JSON-файле (переживает перезапуск; на хостинге подключите постоянный диск и задайте DATA_FILE)
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
let db = { bans: {}, strikes: {}, reports: {}, adults: {} };
try { db = { ...db, ...JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) }; } catch {}
let saveT;
const save = () => {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    fs.writeFile(DATA_FILE + '.tmp', JSON.stringify(db), (e) => { if (!e) fs.rename(DATA_FILE + '.tmp', DATA_FILE, () => {}); });
  }, 500);
};
const DAY = 864e5;
// 0 = не забанен, -1 = навсегда, иначе время окончания бана
function banUntil(uid) {
  const u = db.bans[uid];
  if (!u) return 0;
  if (u !== -1 && u < Date.now()) { delete db.bans[uid]; save(); return 0; }
  return u;
}
// 3 жалобы от РАЗНЫХ людей за 7 дней -> бан: 1-й раз сутки, 2-й раз неделя, 3-й навсегда
function addReport(from, to) {
  const now = Date.now();
  const list = (db.reports[to] || []).filter((r) => now - r.at < 7 * DAY);
  if (list.some((r) => r.by === from)) return;
  list.push({ by: from, at: now });
  if (list.length >= 3) {
    const strikes = (db.strikes[to] || 0) + 1;
    db.strikes[to] = strikes;
    db.bans[to] = strikes >= 3 ? -1 : now + (strikes === 1 ? 1 : 7) * DAY;
    list.length = 0;
  }
  db.reports[to] = list;
  save();
}
const GENDERS = ['m', 'f'];
const LOOKING = ['m', 'f', 'any'];

const send = (c, msg) => c.readyState === 1 && c.send(JSON.stringify(msg));

// Взаимная совместимость: я подхожу ему, а он мне
const compatible = (a, b) =>
  a.mode === b.mode &&
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

// Счётчик онлайна: рассылаем всем при входе/выходе и раз в 5 секунд
const broadcastOnline = () => {
  const m = JSON.stringify({ t: 'online', n: wss.clients.size });
  wss.clients.forEach((c) => c.readyState === 1 && c.send(m));
};
setInterval(broadcastOnline, 5000);

wss.on('connection', (ws) => {
  ws.uid = null;
  ws.peer = null;
  broadcastOnline();

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === 'find') {
      const uid = BOT_TOKEN ? verifyTelegram(m.initData) : 'guest' + String(m.guest || '').slice(0, 32);
      if (!uid) return send(ws, { t: 'error', msg: 'Откройте приложение из Telegram' });
      const until = banUntil(uid);
      if (until) {
        const msg = until === -1 ? 'Доступ закрыт навсегда из-за жалоб' : 'Доступ закрыт из-за жалоб до ' + new Date(until).toLocaleString('ru-RU');
        return send(ws, { t: 'error', msg });
      }
      if (m.adult !== true) return send(ws, { t: 'error', msg: 'Нужно подтвердить, что вам исполнилось 18 лет' });
      if (!db.adults[uid]) { db.adults[uid] = Date.now(); save(); }
      const now = Date.now();
      ws.fl = (ws.fl || []).filter((t) => now - t < 60000);
      if (ws.fl.length >= 30) return send(ws, { t: 'cooldown' });
      ws.fl.push(now);
      if (!GENDERS.includes(m.gender) || !LOOKING.includes(m.looking)) return;
      ws.uid = uid;
      ws.gender = m.gender;
      ws.looking = m.looking;
      ws.mode = m.mode === 'v' ? 'v' : 'a'; // 'a' = голос, 'v' = видео; пары только внутри одного режима
      unlink(ws);
      enqueue(ws);
    } else if (m.t === 'signal' && ws.peer) {
      send(ws.peer, { t: 'signal', data: m.data });
    } else if (m.t === 'stop') {
      unlink(ws);
    } else if (m.t === 'report' && ws.peer) {
      addReport(ws.uid, ws.peer.uid);
      unlink(ws);
    }
  });

  ws.on('close', () => { unlink(ws); broadcastOnline(); });
});

server.listen(PORT, () => console.log('Listening on ' + PORT));
