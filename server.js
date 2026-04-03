// CERBERUS BACKEND — TOKEN AUTH SYSTEM
const express = require('express');
const http    = require('http');
const WebSocket = require('ws');
const crypto  = require('crypto');
const app     = express();
const server  = http.createServer(app);
const wss     = new WebSocket.Server({ server });
app.use(express.json({ limit: '10kb' }));

const API_KEY = process.env.API_KEY;
const LRM_KEY = process.env.LRM_KEY;
const LRM_PID = process.env.LRM_PID;

if (!API_KEY || !LRM_KEY || !LRM_PID) {
  console.error("Missing environment variables: API_KEY, LRM_KEY, or LRM_PID");
  process.exit(1);
}

app.get('/', (_req, res) => res.send('online'));

// TEMP: get server outbound IP for Luarmor whitelist
app.get('/myip', async (_req, res) => {
  try {
    const r = await fetch('https://ifconfig.me/ip');
    const ip = await r.text();
    res.send(ip);
  } catch(e) {
    res.send('failed: ' + e.message);
  }
});

const tokens = new Map();

function generateToken(userKey) {
  const token   = crypto.randomBytes(32).toString('hex');
  const expires = Date.now() + 60_000;
  tokens.set(token, { userKey, expires });
  setTimeout(() => tokens.delete(token), 60_000);
  return token;
}

function consumeToken(token) {
  const entry = tokens.get(token);
  if (!entry) return null;
  if (Date.now() > entry.expires) { tokens.delete(token); return null; }
  tokens.delete(token);
  return entry.userKey;
}

async function isKeyValid(key) {
  try {
    const url = `https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(key)}`;
    console.log("[DEBUG] Checking key:", key);
    console.log("[DEBUG] LRM_PID:", LRM_PID);
    const r = await fetch(url, {
      headers: {
        'Authorization': LRM_KEY.trim(),
        'Content-Type': 'application/json'
      }
    });
    console.log("[DEBUG] Luarmor HTTP status:", r.status);
    if (!r.ok) {
      const text = await r.text();
      console.log("[DEBUG] Luarmor error body:", text.slice(0, 200));
      return false;
    }
    const d = await r.json();
    console.log("[DEBUG] Luarmor response:", JSON.stringify(d));
    if (!d.success || !d.users?.length) return false;
    const u = d.users[0];
    if (u.banned) { console.log("[DEBUG] Key is banned"); return false; }
    if (u.auth_expire !== -1 && u.auth_expire < Math.floor(Date.now() / 1000)) {
      console.log("[DEBUG] Key is expired"); return false;
    }
    return true;
  } catch(e) {
    console.log("[DEBUG] isKeyValid error:", e.message);
    return false;
  }
}

app.get('/get_token', async (req, res) => {
  const userKey = req.query.user_key;
  if (!userKey) return res.status(400).json({ error: 'Missing user_key' });
  const valid = await isKeyValid(userKey);
  console.log("[DEBUG] /get_token isKeyValid result:", valid);
  if (!valid) return res.status(403).json({ error: 'Invalid or expired key' });
  const token = generateToken(userKey);
  res.json({ token });
});

function broadcast(obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(buf);
  }
}

const jobPresence = {};
const clientKeys  = new Map();

// Check keys every 60s instead of 5s to avoid Luarmor rate limits
setInterval(async () => {
  for (const [ws, key] of clientKeys.entries()) {
    if (ws.readyState !== WebSocket.OPEN) { clientKeys.delete(ws); continue; }
    if (!await isKeyValid(key)) {
      try { ws.send(JSON.stringify({ type: 'expired' })); } catch (_) {}
      ws.close(4001, 'Key expired');
      clientKeys.delete(ws);
    }
  }
}, 60_000);

wss.on('connection', async (ws, req) => {
  const rawUrl = req.url || '/';
  const qIndex = rawUrl.indexOf('?');
  const params = qIndex >= 0 ? new URLSearchParams(rawUrl.slice(qIndex + 1)) : new URLSearchParams();

  const token   = params.get('token');
  const userKey = token ? consumeToken(token) : null;

  if (!userKey) {
    try { ws.send(JSON.stringify({ type: 'expired' })); } catch (_) {}
    ws.close(4001, 'Unauthorized');
    return;
  }

  clientKeys.set(ws, userKey);

  let _username = null;
  let _jobId    = null;

  ws.on('message', data => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'presence_join' && msg.username && msg.job_id) {
      _username = msg.username;
      _jobId    = msg.job_id;
      if (jobPresence[_jobId]) {
        for (const existingUser of jobPresence[_jobId]) {
          if (existingUser !== _username) {
            try { ws.send(JSON.stringify({ type: 'presence_join', username: existingUser, job_id: _jobId })); } catch(_) {}
          }
        }
      }
      if (!jobPresence[_jobId]) jobPresence[_jobId] = new Set();
      jobPresence[_jobId].add(_username);
      broadcast({ type: 'presence_join', username: _username, job_id: _jobId });
      return;
    }

    broadcast(msg);
  });

  ws.on('close', () => {
    clientKeys.delete(ws);
    if (_username && _jobId) {
      if (jobPresence[_jobId]) {
        jobPresence[_jobId].delete(_username);
        if (jobPresence[_jobId].size === 0) delete jobPresence[_jobId];
      }
      broadcast({ type: 'presence_leave', username: _username, job_id: _jobId });
    }
  });
});

app.post('/submit', (req, res) => {
  if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const b = req.body;
  if (!b?.name) return res.status(400).json({ error: 'Missing name' });
  broadcast({
    type:     'brainrot',
    name:     b.name,
    gen:      b.gen      || '?',
    mutation: b.mutation || 'None',
    value:    b.value    || 0,
    job_id:   b.job_id   || '',
    place_id: b.place_id || ''
  });
  res.json({ ok: true });
});

// Ping clients every 20s to keep connections alive
setInterval(() => {
  const buf = Buffer.from(JSON.stringify({ type: 'ping' }));
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(buf);
  }
}, 20_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Cerberus backend running on port', PORT));
