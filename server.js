// ══════════════════════════════════════════════════
//  CERBERUS — server.js
// ══════════════════════════════════════════════════
'use strict';

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const crypto    = require('crypto');
const axios     = require('axios');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, perMessageDeflate: false });
app.use(express.json({ limit: '50kb' }));

// ─── PROXY ───────────────────────────────────────
const PROXY_URL  = process.env.PROXY_URL || '';
const proxyMatch = PROXY_URL.match(/http:\/\/([^:]+):([^@]+)@([^:]+):(\d+)/);
const axiosProxy = proxyMatch ? {
    host:     proxyMatch[3],
    port:     parseInt(proxyMatch[4]),
    auth:     { username: proxyMatch[1], password: proxyMatch[2] },
    protocol: 'http',
} : undefined;

async function luarmorFetch(url, options = {}) {
    const method  = (options.method || 'GET').toLowerCase();
    const headers = options.headers || {};
    const data    = options.body ? JSON.parse(options.body) : undefined;
    try {
        const res = await axios({ method, url, headers, data, proxy: axiosProxy, timeout: 10_000 });
        return {
            ok:     true,
            status: res.status,
            json:   async () => res.data,
            text:   async () => JSON.stringify(res.data),
        };
    } catch (e) {
        const status = e.response?.status || 500;
        const body   = e.response?.data   || e.message;
        return {
            ok:   false,
            status,
            json: async () => body,
            text: async () => (typeof body === 'string' ? body : JSON.stringify(body)),
        };
    }
}

// ─── ENV ─────────────────────────────────────────
const API_KEY    = process.env.API_KEY;
const LRM_KEY    = process.env.LRM_KEY;
const LRM_PID    = process.env.LRM_PID;
const BOT_TOKEN  = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;
const FETCHER_URL = process.env.FETCHER_URL || 'http://82.11.247.18:5000';
const MAX_SLOTS  = 7;
const LOGO_URL   = 'https://media.discordapp.net/attachments/1487763701040680971/1489669239202644089/image.png';
const ADMIN_IDS  = new Set(['1405960794503647324']);

console.log('[ENV] API_KEY set:',    !!API_KEY);
console.log('[ENV] LRM_KEY set:',    !!LRM_KEY);
console.log('[ENV] LRM_PID:',         LRM_PID);
console.log('[ENV] BOT_TOKEN set:',  !!BOT_TOKEN);
console.log('[ENV] CHANNEL_ID:',      CHANNEL_ID);
console.log('[ENV] FETCHER_URL:',     FETCHER_URL);
if (!process.env.WEBHOOK_50_400M)    console.warn('[ENV] WARNING: WEBHOOK_50_400M not set');
if (!process.env.WEBHOOK_400_999M)   console.warn('[ENV] WARNING: WEBHOOK_400_999M not set');
if (!process.env.WEBHOOK_999M_PLUS)  console.warn('[ENV] WARNING: WEBHOOK_999M_PLUS not set');
if (!process.env.WEBHOOK_EXECUTIONS) console.warn('[ENV] WARNING: WEBHOOK_EXECUTIONS not set');

if (!API_KEY || !LRM_KEY || !LRM_PID || !BOT_TOKEN || !CHANNEL_ID) {
    console.error('[ENV] Missing required env vars — exiting');
    process.exit(1);
}

app.get('/', (_req, res) => res.send('online'));

// ─── LUARMOR — CACHED USER LIST ──────────────────
let _usersCache   = null;
let _usersCacheAt = 0;
const USERS_TTL   = 30_000;

async function getAllUsers(forceRefresh = false) {
    const now = Date.now();
    if (!forceRefresh && _usersCache && now - _usersCacheAt < USERS_TTL) return _usersCache;
    try {
        const res = await luarmorFetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
            headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' },
        });
        if (!res.ok) return _usersCache || [];
        const d       = await res.json();
        _usersCache   = d.users || [];
        _usersCacheAt = now;
        return _usersCache;
    } catch (e) {
        console.error('[Luarmor] getAllUsers error:', e.message);
        return _usersCache || [];
    }
}

function invalidateUsersCache() {
    _usersCache   = null;
    _usersCacheAt = 0;
}

async function isKeyValid(key) {
    try {
        const res = await luarmorFetch(
            `https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(key)}`,
            { headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' } },
        );
        if (!res.ok) return { valid: false, user: null };
        const d = await res.json();
        if (!d.success || !d.users?.length) return { valid: false, user: null };
        const u   = d.users[0];
        const now = Math.floor(Date.now() / 1000);
        if (u.banned)                                      return { valid: false, user: u };
        if (u.auth_expire !== -1 && u.auth_expire <= now) return { valid: false, user: u };
        return { valid: true, user: u };
    } catch {
        return { valid: false, user: null };
    }
}

async function createKey(durationSeconds, discordId, label) {
    const auth_expire = Math.floor(Date.now() / 1000) + durationSeconds;
    try {
        const res = await luarmorFetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
            method:  'POST',
            headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ auth_expire, discord_id: discordId, note: `Cerberus — ${label}` }),
        });
        const text = await res.text();
        let d;
        try { d = JSON.parse(text); }
        catch { console.error('[Luarmor] createKey non-JSON:', text.slice(0, 200)); return null; }
        if (!d.success) { console.error('[Luarmor] createKey failed:', JSON.stringify(d)); return null; }
        invalidateUsersCache();
        return d.user_key;
    } catch (e) {
        console.error('[Luarmor] createKey error:', e.message);
        return null;
    }
}

async function revokeKey(userKey) {
    try {
        const res = await luarmorFetch(
            `https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(userKey)}`,
            { method: 'DELETE', headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' } },
        );
        if (res.ok) invalidateUsersCache();
        return res.ok;
    } catch (e) {
        console.error('[Luarmor] revokeKey error:', e.message);
        return false;
    }
}

async function getKeyByDiscordId(discordId) {
    const users = await getAllUsers();
    return users.find(u => u.discord_id === discordId) || null;
}

// ─── TOKEN SYSTEM ────────────────────────────────
const tokens = new Map();

function generateToken(userKey, user) {
    const token   = crypto.randomBytes(32).toString('hex');
    const expires = Date.now() + 60_000;
    tokens.set(token, { userKey, user, expires });
    setTimeout(() => tokens.delete(token), 60_000);
    return token;
}

function consumeToken(token) {
    if (!token) return null;
    const entry = tokens.get(token);
    if (!entry) return null;
    if (Date.now() > entry.expires) { tokens.delete(token); return null; }
    tokens.delete(token);
    return entry;
}

// ─── RATE LIMITING ───────────────────────────────
// ✅ Rate limit per job+pet combo so ALL pets from a scan get through.
//    Same pet from the same job is blocked for 30s to prevent spam.
const petSubmitTimes = new Map();
const globalSubmits  = [];
const PET_COOLDOWN   = 30_000;
const GLOBAL_MAX     = 200;
const GLOBAL_WINDOW  = 3_600_000;

function isRateLimited(jobId, petName) {
    const now  = Date.now();
    const key  = jobId + '|' + (petName || '');
    const last = petSubmitTimes.get(key);
    if (last && now - last < PET_COOLDOWN) return true;

    const cutoff = now - GLOBAL_WINDOW;
    while (globalSubmits.length && globalSubmits[0] < cutoff) globalSubmits.shift();
    if (globalSubmits.length >= GLOBAL_MAX) return true;

    petSubmitTimes.set(key, now);
    globalSubmits.push(now);

    if (petSubmitTimes.size > 5000) {
        for (const [k, t] of petSubmitTimes) {
            if (now - t > PET_COOLDOWN * 2) petSubmitTimes.delete(k);
        }
    }
    return false;
}

// ─── WEBSOCKET HELPERS ───────────────────────────
function wsSend(ws, obj) {
    if (ws.readyState !== WebSocket.OPEN) return;
    try { ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj)); } catch {}
}

function wsKick(ws) {
    wsSend(ws, { type: 'expired' });
    setTimeout(() => { try { ws.terminate(); } catch {} }, 300);
}

const PING_BUF    = Buffer.from(JSON.stringify({ type: 'ping' }));
const EXPIRED_BUF = Buffer.from(JSON.stringify({ type: 'expired' }));

function broadcast(obj, excludeWs = null) {
    const buf = Buffer.from(JSON.stringify(obj));
    for (const client of wss.clients) {
        if (client === excludeWs) continue;
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(buf); } catch {}
        }
    }
}

// ─── WEBHOOK ─────────────────────────────────────
function formatPrice(v) {
    const n = parseFloat(v) || 0;
    if (n >= 1e9) return '$' + (n / 1e9).toFixed(2).replace(/\.?0+$/, '') + 'B';
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1).replace(/\.?0+$/, '') + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(1).replace(/\.?0+$/, '') + 'K';
    return '$' + String(n);
}

function displayGen(gen) {
    if (!gen || gen === '?') return '?';
    return String(gen);
}

function fireWebhook(webhook, embedData) {
    fetch(webhook, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ embeds: [embedData] }),
    }).catch(e => console.warn('[Webhook] Failed:', e.message));
}

// ══════════════════════════════════════════════════
//  BOT JOB TRACKING
// ══════════════════════════════════════════════════
const botJobMap  = new Map();
const botCurrent = new Map();

function botJoin(userKey, jobId) {
    botLeave(userKey);
    botCurrent.set(userKey, jobId);
    if (!botJobMap.has(jobId)) botJobMap.set(jobId, new Set());
    botJobMap.get(jobId).add(userKey);
    console.log(`[BotTrack] ${userKey.slice(0,8)}… joined ${jobId} | occupied: ${botJobMap.size} servers`);
}

function botLeave(userKey) {
    const oldJob = botCurrent.get(userKey);
    if (!oldJob) return;
    botCurrent.delete(userKey);
    const set = botJobMap.get(oldJob);
    if (set) {
        set.delete(userKey);
        if (set.size === 0) botJobMap.delete(oldJob);
    }
}

function isJobOccupied(jobId) {
    const set = botJobMap.get(jobId);
    return set && set.size > 0;
}

async function getNextServerFromFetcher(requestingUserKey) {
    const skipped = [];
    for (let i = 0; i < 10; i++) {
        let data;
        try {
            const res = await axios.get(`${FETCHER_URL}/next_server`, { timeout: 8_000 });
            data = res.data;
        } catch (e) {
            console.warn('[Fetcher proxy] Error:', e.message);
            return null;
        }
        if (!data || data.error || !data.job_id) return null;
        const jobId = data.job_id;
        if (!isJobOccupied(jobId)) {
            console.log(`[Fetcher proxy] Giving ${jobId} to ${requestingUserKey.slice(0,8)}… (skipped ${skipped.length})`);
            return jobId;
        }
        console.log(`[Fetcher proxy] ${jobId} occupied — skipping`);
        skipped.push(jobId);
        try {
            await axios.post(`${FETCHER_URL}/visited`, { job_id: jobId }, { timeout: 4_000 });
        } catch {}
    }
    console.warn('[Fetcher proxy] Could not find unoccupied server after 10 attempts');
    return null;
}

// ══════════════════════════════════════════════════
//  SCAN COORDINATION
// ══════════════════════════════════════════════════
const hopApproved = new Set();

app.post('/scan_done', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const { job_id, found_count, players } = req.body || {};
    if (!job_id) return res.status(400).json({ error: 'Missing job_id' });
    console.log(`[Scan] job=${job_id} found=${found_count ?? 0} players=${players ?? '?'}`);
    broadcast({ type: 'scan_result', job_id, found_count: found_count ?? 0, players: players ?? 0 });
    hopApproved.add(job_id);
    setTimeout(() => hopApproved.delete(job_id), 300_000);
    res.json({ ok: true });
});

app.get('/should_hop', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const jobId = (req.query.job_id || '').trim();
    if (!jobId) return res.status(400).json({ error: 'Missing job_id' });
    if (hopApproved.has(jobId)) {
        hopApproved.delete(jobId);
        return res.json({ hop: true });
    }
    res.json({ hop: false });
});

app.get('/next_server', async (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const userKey = (req.query.user_key || '').trim();
    if (!userKey) return res.status(400).json({ error: 'Missing user_key' });
    const jobId = await getNextServerFromFetcher(userKey);
    if (!jobId) return res.status(503).json({ error: 'no servers available' });
    res.json({ job_id: jobId });
});

app.post('/bot_join', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const { job_id, user_key } = req.body || {};
    if (!job_id || !user_key) return res.status(400).json({ error: 'Missing job_id or user_key' });
    botJoin(user_key, job_id);
    res.json({ ok: true });
});

app.post('/bot_leave', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const { user_key } = req.body || {};
    if (!user_key) return res.status(400).json({ error: 'Missing user_key' });
    botLeave(user_key);
    res.json({ ok: true });
});

// ─── ROUTES ──────────────────────────────────────
app.get('/get_token', async (req, res) => {
    const userKey = (req.query.user_key || '').trim();
    if (!userKey) return res.status(400).json({ error: 'Missing user_key' });
    const { valid, user } = await isKeyValid(userKey);
    if (!valid) return res.status(403).json({ error: 'Invalid or expired key' });
    console.log('[Token] Issued for key:', userKey.slice(0, 8) + '…');
    res.json({ token: generateToken(userKey, user) });
});

app.post('/log_execute', async (req, res) => {
    const userKey = (req.headers['x-api-key'] || '').trim();
    if (!userKey) return res.status(401).json({ error: 'Unauthorized' });
    const { valid, user } = await isKeyValid(userKey);
    if (!valid) return res.status(403).json({ error: 'Invalid or expired key' });
    const { username, userId } = req.body || {};
    if (!username) return res.status(400).json({ error: 'Missing username' });
    const discordId  = user?.discord_id || null;
    const discordTag = discordId ? `<@${discordId}>` : 'Unknown';
    console.log(`[Execute] ${username} (${userId}) — Discord: ${discordId || 'unknown'}`);
    const webhook = process.env.WEBHOOK_EXECUTIONS;
    if (webhook) {
        fireWebhook(webhook, {
            title:  '🎮 Cerberus Execution',
            color:  3066993,
            fields: [
                { name: 'Roblox Username', value: String(username),      inline: true },
                { name: 'User ID',         value: String(userId || '?'), inline: true },
                { name: 'Discord',         value: discordTag,            inline: true },
            ],
        });
    }
    res.json({ ok: true });
});

// ── /submit_batch ─────────────────────────────────
app.post('/submit_batch', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const b = req.body;
    if (!b?.pets || !Array.isArray(b.pets) || b.pets.length === 0)
        return res.status(400).json({ error: 'Missing pets array' });
    const jobId = b.job_id || 'unknown';

    const pets    = [...b.pets].sort((a, bb) => (bb.value || 0) - (a.value || 0));
    const best    = pets[0];
    const bestVal = parseFloat(String(best.value || 0));

    // ✅ Filter out rate-limited pets individually
    const allowed = pets.filter(pet => !isRateLimited(jobId, pet.name));
    if (allowed.length === 0) return res.status(429).json({ error: 'Rate limited' });

    for (const pet of allowed) {
        broadcast({
            type:     'brainrot',
            name:     pet.name,
            gen:      pet.gen || '?',
            mutation: pet.mutation || 'None',
            value:    pet.value || 0,
            job_id:   b.job_id || '',
            place_id: b.place_id || '',
        });
    }

    let webhook = null;
    if      (bestVal >= 999e6) webhook = process.env.WEBHOOK_999M_PLUS;
    else if (bestVal >= 400e6) webhook = process.env.WEBHOOK_400_999M;
    else if (bestVal >= 50e6)  webhook = process.env.WEBHOOK_50_400M;

    console.log(`[Batch] ${allowed.length}/${pets.length} pets | best=${best.name} val=${bestVal} | webhook=${webhook ? 'yes' : 'no'}`);

    if (webhook) {
        const bestMut = best.mutation && best.mutation !== 'None' ? best.mutation : 'Base';
        const color   = bestVal >= 999e6 ? 0xFFD700 : bestVal >= 400e6 ? 0x00BFFF : 0x00AF41;

        let desc = `🏆 **Best**\n[${bestMut}] ${best.name} [${displayGen(best.gen)}]`;
        if (allowed.length > 1) {
            desc += '\n\n♦ **Others**';
            for (const pet of allowed.slice(1)) {
                const mut = pet.mutation && pet.mutation !== 'None' ? pet.mutation : 'Base';
                desc += `\n• [${mut}] ${pet.name} [${displayGen(pet.gen)}]`;
            }
        }
        desc += '\n\n💸 **Buy a Slot!**';

        fireWebhook(webhook, {
            title:       '⭐ Cerberus Notifier | Finds',
            description: desc.slice(0, 3900),
            color,
            thumbnail:   b.image_url ? { url: b.image_url } : undefined,
            fields:      [{ name: 'Players', value: b.players ? `${b.players}/8` : 'Unknown', inline: false }],
            footer:      { text: 'Cerberus Notifier • gg/cerberusnotifier' },
            timestamp:   new Date().toISOString(),
        });
    }
    res.json({ ok: true });
});

// ── /submit (single pet) ─────────────────────────
app.post('/submit', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const b = req.body;
    if (!b?.name) return res.status(400).json({ error: 'Missing name' });
    const jobId = b.job_id || 'unknown';

    // ✅ Rate limit per pet name so all pets from a scan get through
    if (isRateLimited(jobId, b.name)) return res.status(429).json({ error: 'Rate limited' });

    broadcast({
        type: 'brainrot', name: b.name,
        gen: b.gen || '?', mutation: b.mutation || 'None',
        value: b.value || 0, job_id: b.job_id || '', place_id: b.place_id || '',
    });

    const val = parseFloat(String(b.value || 0));
    let webhook = null;
    if      (val >= 999e6) webhook = process.env.WEBHOOK_999M_PLUS;
    else if (val >= 400e6) webhook = process.env.WEBHOOK_400_999M;
    else if (val >= 50e6)  webhook = process.env.WEBHOOK_50_400M;

    console.log(`[Submit] ${b.name} | gen=${b.gen} | val=${val} | webhook=${webhook ? 'yes' : 'no'}`);

    if (webhook) {
        const mut = b.mutation && b.mutation !== 'None' ? b.mutation : 'Base';
        fireWebhook(webhook, {
            title:       '⭐ Cerberus Notifier | Find',
            description: `🏆 **Best**\n[${mut}] ${b.name} [${displayGen(b.gen)}]\n\n💸 **Buy a Slot!**`,
            color:       val >= 999e6 ? 0xFFD700 : val >= 400e6 ? 0x00BFFF : 0x00AF41,
            thumbnail:   b.image_url ? { url: b.image_url } : undefined,
            fields:      [{ name: 'Players', value: b.players ? `${b.players}/8` : 'Unknown', inline: false }],
            footer:      { text: 'Cerberus Notifier • gg/cerberusnotifier' },
            timestamp:   new Date().toISOString(),
        });
    }
    res.json({ ok: true });
});

// ══════════════════════════════════════════════════
//  WEBSOCKET
// ══════════════════════════════════════════════════
const jobPresence = {};

function presenceJoin(jobId, username) {
    if (!jobPresence[jobId]) jobPresence[jobId] = new Set();
    jobPresence[jobId].add(username);
}

function presenceLeave(jobId, username) {
    if (!jobPresence[jobId]) return;
    jobPresence[jobId].delete(username);
    if (jobPresence[jobId].size === 0) delete jobPresence[jobId];
}

wss.on('connection', async (ws, req) => {
    const rawUrl = req.url || '/';
    let token    = null;
    const qi     = rawUrl.indexOf('?');
    if (qi >= 0) {
        try { token = new URLSearchParams(rawUrl.slice(qi + 1)).get('token') || null; } catch {}
    }
    if (!token) {
        const proto = req.headers['sec-websocket-protocol'];
        if (proto) token = proto.split(',')[0].trim() || null;
    }

    const entry = consumeToken(token);
    if (!entry) {
        try { ws.send(EXPIRED_BUF); } catch {}
        ws.terminate();
        return;
    }

    const { userKey, user } = entry;
    ws._cerberusKey = userKey;
    ws._authExpire  = user ? user.auth_expire : null;
    ws.isAlive      = true;

    console.log('[WS] Connected:', userKey.slice(0, 8) + '…');

    let expiryTimer = null;
    if (user && user.auth_expire !== -1) {
        const secsLeft = user.auth_expire - Math.floor(Date.now() / 1000);
        if (secsLeft > 0) {
            expiryTimer = setTimeout(() => {
                console.log('[WS] Key expired, kicking:', userKey.slice(0, 8) + '…');
                wsKick(ws);
            }, secsLeft * 1000);
        } else {
            wsKick(ws);
            return;
        }
    }

    let _username = null;
    let _jobId    = null;

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('error', err => console.warn('[WS] Socket error:', err.message));

    ws.on('message', data => {
        let msg;
        try { msg = JSON.parse(data); } catch { return; }
        if (!msg || typeof msg !== 'object') return;

        if (msg.type === 'presence_join' && msg.username && msg.job_id) {
            _username = msg.username;
            _jobId    = msg.job_id;
            if (jobPresence[_jobId]) {
                for (const existing of jobPresence[_jobId]) {
                    if (existing !== _username) {
                        wsSend(ws, { type: 'presence_join', username: existing, job_id: _jobId });
                    }
                }
            }
            presenceJoin(_jobId, _username);
            broadcast({ type: 'presence_join', username: _username, job_id: _jobId }, ws);
            return;
        }

        broadcast(msg);
    });

    ws.on('close', () => {
        if (expiryTimer) clearTimeout(expiryTimer);
        botLeave(userKey);
        if (_username && _jobId) {
            presenceLeave(_jobId, _username);
            broadcast({ type: 'presence_leave', username: _username, job_id: _jobId }, ws);
        }
    });
});

// ─── HEARTBEAT ───────────────────────────────────
const heartbeatInterval = setInterval(() => {
    for (const ws of wss.clients) {
        if (!ws.isAlive) { ws.terminate(); continue; }
        ws.isAlive = false;
        try { ws.ping(); } catch {}
    }
}, 30_000);

wss.on('close', () => clearInterval(heartbeatInterval));

// ─── FALLBACK WATCHER ────────────────────────────
setInterval(() => {
    if (wss.clients.size === 0) return;
    const now = Math.floor(Date.now() / 1000);
    for (const ws of wss.clients) {
        if (ws.readyState !== WebSocket.OPEN) continue;
        if (ws._authExpire !== -1 && ws._authExpire && ws._authExpire <= now) {
            console.log('[Watcher] In-memory expiry kick:', ws._cerberusKey?.slice(0, 8) + '…');
            wsKick(ws);
        }
    }
}, 5_000);

setInterval(async () => {
    if (wss.clients.size === 0) return;
    const now     = Math.floor(Date.now() / 1000);
    const users   = await getAllUsers(true);
    const userMap = new Map(users.map(u => [u.user_key, u]));
    for (const ws of wss.clients) {
        if (ws.readyState !== WebSocket.OPEN || !ws._cerberusKey) continue;
        const u     = userMap.get(ws._cerberusKey);
        const valid = u && !u.banned && (u.auth_expire === -1 || u.auth_expire > now);
        if (!valid) {
            console.log('[Watcher] Luarmor kick:', ws._cerberusKey?.slice(0, 8) + '…');
            wsKick(ws);
        }
        if (u) ws._authExpire = u.auth_expire;
    }
}, 60_000);

// ─── JSON PING ───────────────────────────────────
setInterval(() => {
    for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(PING_BUF); } catch {}
        }
    }
}, 20_000);

// ══════════════════════════════════════════════════
//  STARTUP
// ══════════════════════════════════════════════════
function kickLiveSockets(userKey) {
    for (const client of wss.clients) {
        if (client._cerberusKey === userKey && client.readyState === WebSocket.OPEN) {
            wsKick(client);
        }
    }
}

async function rescheduleExpiryTimers() {
    console.log('[Startup] Re-scheduling expiry timers…');
    const now   = Math.floor(Date.now() / 1000);
    const users = await getAllUsers(true);
    let   count = 0;
    for (const u of users) {
        if (u.banned) continue;
        if (u.auth_expire === -1) continue;
        const secsLeft = u.auth_expire - now;
        if (secsLeft <= 0) continue;
        setTimeout(() => {
            console.log('[Expiry] Startup timer fired:', u.user_key?.slice(0, 8) + '…');
            kickLiveSockets(u.user_key);
            schedulePanel(500);
        }, secsLeft * 1000);
        count++;
    }
    console.log(`[Startup] Scheduled ${count} expiry timer(s)`);
}

// ══════════════════════════════════════════════════
//  DISCORD BOT
// ══════════════════════════════════════════════════
let panelMessageId = null;
let panelDebounce  = null;
let sequence       = null;
let heartbeatGW;
let gatewayWs;

function parseDuration(str) {
    const match = str.match(/^(\d+)(h|d|w|m)$/i);
    if (!match) return null;
    const num  = parseInt(match[1]);
    const unit = match[2].toLowerCase();
    if (unit === 'h') return { seconds: num * 3_600,     label: `${num} hour${num !== 1 ? 's' : ''}` };
    if (unit === 'd') return { seconds: num * 86_400,    label: `${num} day${num !== 1 ? 's' : ''}` };
    if (unit === 'w') return { seconds: num * 604_800,   label: `${num} week${num !== 1 ? 's' : ''}` };
    if (unit === 'm') return { seconds: num * 2_592_000, label: `${num} month${num !== 1 ? 's' : ''}` };
    return null;
}

function formatTime(secs) {
    if (secs <= 0) return 'Expired';
    const d = Math.floor(secs / 86_400);
    const h = Math.floor((secs % 86_400) / 3_600);
    const m = Math.floor((secs % 3_600) / 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

async function discordRequest(method, path, body) {
    try {
        const res = await fetch(`https://discord.com/api/v10${path}`, {
            method,
            headers: { Authorization: `Bot ${BOT_TOKEN}`, 'Content-Type': 'application/json' },
            body: body ? JSON.stringify(body) : undefined,
        });
        if (!res.ok) {
            const t = await res.text();
            console.warn(`[Discord] ${method} ${path} failed:`, t.slice(0, 200));
            return null;
        }
        return res.json();
    } catch (e) {
        console.warn(`[Discord] ${method} ${path} threw:`, e.message);
        return null;
    }
}

function schedulePanel(delayMs = 1000) {
    if (panelDebounce) clearTimeout(panelDebounce);
    panelDebounce = setTimeout(() => { panelDebounce = null; updatePanel(); }, delayMs);
}

async function updatePanel() {
    const now    = Math.floor(Date.now() / 1000);
    const users  = await getAllUsers(true);
    const active = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
    const used   = active.length;
    const full   = used >= MAX_SLOTS;
    const lines  = active.length > 0
        ? active.map((u, i) => {
            const tag  = u.discord_id ? `<@${u.discord_id}>` : `\`${u.user_key.slice(0, 8)}…\``;
            const time = u.auth_expire === -1 ? '∞' : formatTime(u.auth_expire - now);
            return `${i + 1}. ${tag} → ${time}`;
        }).join('\n')
        : '*No active slots.*';
    const embed = {
        title:       `🔴 Cerberus Notifier Active Slots (${used}/${MAX_SLOTS})`,
        description: lines + (full ? '\n\n**All slots are full at the moment.**' : ''),
        color:       full ? 0xDE3163 : 0x00AF41,
        thumbnail:   { url: LOGO_URL },
        footer:      { text: 'Cerberus Notifier • gg/cerberusnotifier' },
        timestamp:   new Date().toISOString(),
        fields: [{
            name:   full ? '⛔ All slots are full' : `✅ ${MAX_SLOTS - used} slot${MAX_SLOTS - used !== 1 ? 's' : ''} available`,
            value:  full ? 'Check back later or create a ticket for waitlist' : 'Create a ticket to purchase a slot',
            inline: false,
        }],
    };
    if (!panelMessageId) {
        const msg = await discordRequest('POST', `/channels/${CHANNEL_ID}/messages`, { embeds: [embed] });
        if (msg) { panelMessageId = msg.id; console.log('[Panel] Posted:', panelMessageId); }
    } else {
        const result = await discordRequest('PATCH', `/channels/${CHANNEL_ID}/messages/${panelMessageId}`, { embeds: [embed] });
        if (!result) {
            panelMessageId = null;
            const msg = await discordRequest('POST', `/channels/${CHANNEL_ID}/messages`, { embeds: [embed] });
            if (msg) { panelMessageId = msg.id; console.log('[Panel] Re-posted:', panelMessageId); }
        } else {
            console.log('[Panel] Updated at', new Date().toLocaleTimeString());
        }
    }
}

async function handleMessage(msg) {
    if (msg.author?.bot) return;
    const content = msg.content?.trim();
    if (!content?.startsWith('!')) return;
    if (!ADMIN_IDS.has(msg.author.id)) return;
    const parts = content.split(/\s+/).filter(Boolean);
    const cmd   = parts[0].toLowerCase();

    if (cmd === '!addslot') {
        const mention     = parts[1];
        const durationStr = parts[2];
        if (!mention || !durationStr) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Usage: `!addslot @user <duration>` e.g. `!addslot @user 1d`',
            });
        }
        const duration = parseDuration(durationStr);
        if (!duration || duration.seconds < 7200) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Minimum duration is 2h. Use `2h`, `1d`, `1w`, `1m`',
            });
        }
        const discordId = mention.replace(/[<@!>]/g, '');
        const now       = Math.floor(Date.now() / 1000);
        const users     = await getAllUsers(true);
        const active    = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
        if (active.length >= MAX_SLOTS) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: '❌ All slots are full!' });
        }
        const existing = await getKeyByDiscordId(discordId);
        if (existing && (existing.auth_expire === -1 || existing.auth_expire > now)) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: `❌ <@${discordId}> already has an active slot.`,
            });
        }
        const key = await createKey(duration.seconds, discordId, duration.label);
        if (!key) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Failed to create key on Luarmor.',
            });
        }
        setTimeout(() => {
            console.log('[Expiry] Timer fired for key:', key.slice(0, 8) + '…');
            kickLiveSockets(key);
            schedulePanel(500);
        }, duration.seconds * 1000);
        const dmChannel = await discordRequest('POST', '/users/@me/channels', { recipient_id: discordId });
        if (dmChannel) {
            await discordRequest('POST', `/channels/${dmChannel.id}/messages`, {
                embeds: [{
                    title:       '🐕 Cerberus Notifier — Your Key',
                    description: `Your slot is active for **${duration.label}**.\n\nHead to the 📡・finder-panel in the Discord server and redeem your key to get started.`,
                    color:       0x00AF41,
                    thumbnail:   { url: LOGO_URL },
                    fields: [
                        { name: '🔑 Your Key', value: `\`${key}\``,    inline: false },
                        { name: '⏰ Duration',  value: duration.label, inline: true  },
                    ],
                    footer: { text: 'Cerberus Notifier • gg/cerberusnotifier' },
                }],
            });
        }
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            content: `✅ Slot added for <@${discordId}> — **${duration.label}**. Key sent via DM.`,
        });
        schedulePanel(500);
        return;
    }

    if (cmd === '!removeslot') {
        const mention = parts[1];
        if (!mention) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Usage: `!removeslot @user`',
            });
        }
        const discordId = mention.replace(/[<@!>]/g, '');
        const user      = await getKeyByDiscordId(discordId);
        if (!user) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: `❌ No key found for <@${discordId}>.`,
            });
        }
        await revokeKey(user.user_key);
        kickLiveSockets(user.user_key);
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            content: `✅ Slot removed for <@${discordId}> — kicked from WebSocket instantly.`,
        });
        schedulePanel(500);
        return;
    }

    if (cmd === '!slots') {
        const now    = Math.floor(Date.now() / 1000);
        const users  = await getAllUsers();
        const active = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            content: `📊 Active slots: **${active.length}/${MAX_SLOTS}**`,
        });
        return;
    }

    if (cmd === '!help') {
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            embeds: [{
                title:  '🐕 Cerberus Bot Commands',
                color:  0x00AF41,
                fields: [
                    { name: '!addslot @user <duration>', value: 'Add a slot. e.g. `2h` `1d` `1w` `1m`',        inline: false },
                    { name: '!removeslot @user',         value: 'Remove a slot — kicks live session instantly', inline: false },
                    { name: '!slots',                    value: 'Show active slot count',                       inline: false },
                ],
            }],
        });
    }
}

function startGateway() {
    gatewayWs = new WebSocket('wss://gateway.discord.gg/?v=10&encoding=json');
    gatewayWs.on('open', () => console.log('[Gateway] Connected'));
    gatewayWs.on('message', async data => {
        let payload;
        try { payload = JSON.parse(data); } catch { return; }
        const { op, d, t, s } = payload;
        if (s) sequence = s;
        if (op === 10) {
            heartbeatGW = setInterval(() => {
                gatewayWs.send(JSON.stringify({ op: 1, d: sequence }));
            }, d.heartbeat_interval);
            gatewayWs.send(JSON.stringify({
                op: 2,
                d: {
                    token:      BOT_TOKEN,
                    intents:    33280,
                    properties: { os: 'linux', browser: 'cerberus', device: 'cerberus' },
                },
            }));
        }
        if (op === 0 && t === 'READY') {
            console.log('[Gateway] Bot ready:', d.user.username);
            await rescheduleExpiryTimers();
            updatePanel();
        }
        if (op === 0 && t === 'MESSAGE_CREATE') {
            await handleMessage(d);
        }
    });
    gatewayWs.on('close', code => {
        console.warn('[Gateway] Closed:', code, '— reconnecting in 5s');
        clearInterval(heartbeatGW);
        setTimeout(startGateway, 5000);
    });
    gatewayWs.on('error', err => console.error('[Gateway] Error:', err.message));
}

startGateway();
setInterval(updatePanel, 60_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`[Cerberus] Running on port ${PORT}`));
