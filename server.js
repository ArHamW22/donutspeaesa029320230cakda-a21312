// ══════════════════════════════════════════════════
//  CERBERUS — server.js  (reliability build v2)
//
//  FIXES vs previous build:
//  1. !addslot: passes key_days to Luarmor instead of auth_expire
//     (works for both key_days and auth_expire project types).
//  2. Webhook embeds: cleaner KaWaifu-style layout, removed
//     "Buy a Slot!" line, purple accent, image thumbnail.
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

const API_KEY     = process.env.API_KEY;
const LRM_KEY     = process.env.LRM_KEY;
const LRM_PID     = process.env.LRM_PID;
const BOT_TOKEN   = process.env.BOT_TOKEN;
const CHANNEL_ID  = process.env.CHANNEL_ID;
const FETCHER_URL = process.env.FETCHER_URL || 'http://82.11.247.18:5000';
const MAX_SLOTS   = 7;
const LOGO_URL    = 'https://media.discordapp.net/attachments/1487763701040680971/1489669239202644089/image.png';
const ADMIN_IDS   = new Set(['1405960794503647324']);

if (!API_KEY || !LRM_KEY || !LRM_PID || !BOT_TOKEN || !CHANNEL_ID) {
    console.error('[ENV] Missing required env vars — exiting');
    process.exit(1);
}

app.get('/', (_req, res) => res.send('online'));

app.get('/test_broadcast', (req, res) => {
    const count = wss.clients.size;
    broadcast({
        type:     'brainrot',
        name:     'TEST PET (debug)',
        gen:      '$999M/s',
        mutation: 'Rainbow',
        value:    999000000,
        job_id:   'test-job-123',
        place_id: '0',
    });
    res.send(`Broadcast sent to ${count} connected client(s). Check your notifier GUI.`);
});

function parseGen(raw) {
    if (!raw || raw === '?' || raw === '') return 0;
    const s = String(raw).toLowerCase().replace(/[\$,\s%]/g, '').replace(/\/s$/, '');
    const n = parseFloat(s.match(/-?\d+\.?\d*/)?.[0] || '0') || 0;
    if (s.includes('b')) return n * 1e9;
    if (s.includes('m')) return n * 1e6;
    if (s.includes('k')) return n * 1e3;
    return n;
}

async function fetchWikiImage(petName) {
    try {
        const encoded = encodeURIComponent(petName.replace(/ /g, '_'));
        const url = `https://stealabrainrot.fandom.com/api.php?action=query&prop=pageimages&format=json&piprop=thumbnail&pithumbsize=500&titles=${encoded}`;
        const res = await axios.get(url, { timeout: 8_000 });
        const pages = res.data?.query?.pages;
        if (pages) {
            for (const page of Object.values(pages)) {
                if (page.thumbnail?.source) return page.thumbnail.source;
            }
        }
    } catch {}
    return null;
}

// ─── USER CACHE ──────────────────────────────────
let _usersCache   = null;
let _usersCacheAt = 0;
const USERS_TTL   = 45_000;

async function getAllUsers(forceRefresh = false) {
    const now = Date.now();
    if (!forceRefresh && _usersCache && now - _usersCacheAt < USERS_TTL) return _usersCache;
    try {
        const res = await luarmorFetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
            headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' },
        });
        if (!res.ok) {
            console.warn('[Luarmor] getAllUsers failed status=' + res.status + ' — returning stale cache');
            return _usersCache || [];
        }
        const d       = await res.json();
        _usersCache   = d.users || [];
        _usersCacheAt = now;
        return _usersCache;
    } catch (err) {
        console.warn('[Luarmor] getAllUsers threw:', err.message, '— returning stale cache');
        return _usersCache || [];
    }
}

function invalidateUsersCache() {
    _usersCache   = null;
    _usersCacheAt = 0;
}

// ─── KEY VALIDATION ──────────────────────────────
async function isKeyValid(key, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const res = await luarmorFetch(
                `https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(key)}`,
                { headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' } },
            );
            if (!res.ok) {
                if (res.status === 401 || res.status === 403) return { valid: false, user: null, reason: 'unauthorized' };
                if (attempt < maxRetries) { await sleep(800 * attempt); continue; }
                return { valid: false, user: null, reason: 'http_' + res.status };
            }
            const d = await res.json();
            if (!d.success || !Array.isArray(d.users) || d.users.length === 0) {
                if (attempt < maxRetries) {
                    console.log(`[Luarmor] isKeyValid: empty result attempt ${attempt}/${maxRetries}, retrying`);
                    await sleep(800 * attempt);
                    continue;
                }
                return { valid: false, user: null, reason: 'not_found' };
            }
            const u   = d.users[0];
            const now = Math.floor(Date.now() / 1000);
            if (u.banned) return { valid: false, user: u, reason: 'banned' };
            if (u.auth_expire !== -1 && u.auth_expire !== 0 && u.auth_expire <= now) {
                return { valid: false, user: u, reason: 'expired' };
            }
            return { valid: true, user: u };
        } catch (err) {
            if (attempt < maxRetries) {
                console.warn(`[Luarmor] isKeyValid threw attempt ${attempt}:`, err.message);
                await sleep(800 * attempt);
            } else {
                return { valid: false, user: null, reason: 'exception' };
            }
        }
    }
    return { valid: false, user: null, reason: 'exhausted' };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── createKey: auth_expire only (per Luarmor docs) ──────────────
// Per docs: providing discord_id + auth_expire directly starts the timer
// immediately without needing key_days. key_days is NOT sent because it
// only accepts whole days, so 2h would silently become 1 day minimum.
async function createKey(durationSeconds, discordId, label) {
    // Use auth_expire (unix timestamp) only — key_days only accepts whole days
    // so passing it would round 2h up to 1 day. auth_expire is precise.
    const auth_expire = Math.floor(Date.now() / 1000) + durationSeconds;

    try {
        const res = await luarmorFetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
            method:  'POST',
            headers: { Authorization: LRM_KEY, 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                auth_expire,
                discord_id: discordId,
                note: `Cerberus — ${label}`,
            }),
        });
        const text = await res.text();

        // Log the raw Luarmor response so you can debug if it fails again
        console.log('[createKey] Luarmor status=' + res.status + ' body=' + text.slice(0, 300));

        let d;
        try { d = JSON.parse(text); } catch { return null; }
        if (!d.success) {
            console.warn('[createKey] Luarmor returned success=false:', d);
            return null;
        }
        invalidateUsersCache();
        return d.user_key;
    } catch (err) {
        console.error('[createKey] threw:', err.message);
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
    } catch {
        return false;
    }
}

async function getKeyByDiscordId(discordId) {
    // Force refresh so we don't get false positives from stale cache
    const users = await getAllUsers(true);
    return users.find(u => u.discord_id === discordId) || null;
}

// ─── TOKEN SYSTEM ────────────────────────────────
const TOKEN_TTL = 10 * 60 * 1000;
const tokens    = new Map();

function generateToken(userKey, user) {
    for (const [tok, entry] of tokens) {
        if (entry.userKey === userKey) tokens.delete(tok);
    }
    const token   = crypto.randomBytes(32).toString('hex');
    const expires = Date.now() + TOKEN_TTL;
    tokens.set(token, { userKey, user, expires });
    setTimeout(() => tokens.delete(token), TOKEN_TTL);
    return token;
}

function consumeToken(token) {
    if (!token) return null;
    const entry = tokens.get(token);
    if (!entry) return { notFound: true };
    if (Date.now() > entry.expires) {
        tokens.delete(token);
        return { notFound: true };
    }
    return entry;
}

// ─── RATE LIMITING ───────────────────────────────
const serverSubmitTimes = new Map();
const globalSubmits     = [];
const SERVER_COOLDOWN   = 30_000;
const GLOBAL_MAX        = 200;
const GLOBAL_WINDOW     = 3_600_000;

function isServerRateLimited(jobId) {
    const now  = Date.now();
    const last = serverSubmitTimes.get(jobId);
    if (last && now - last < SERVER_COOLDOWN) return true;
    const cutoff = now - GLOBAL_WINDOW;
    while (globalSubmits.length && globalSubmits[0] < cutoff) globalSubmits.shift();
    // Record timestamp first so per-server cooldown applies regardless of global limit
    serverSubmitTimes.set(jobId, now);
    if (globalSubmits.length >= GLOBAL_MAX) return true;
    globalSubmits.push(now);
    if (serverSubmitTimes.size > 5000) {
        for (const [k, t] of serverSubmitTimes) {
            if (now - t > SERVER_COOLDOWN * 2) serverSubmitTimes.delete(k);
        }
    }
    return false;
}

// ─── WS HELPERS ──────────────────────────────────
function wsSend(ws, obj) {
    if (ws.readyState !== WebSocket.OPEN) return;
    try { ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj)); } catch {}
}

function wsKick(ws) {
    wsSend(ws, { type: 'expired' });
    setTimeout(() => { try { ws.terminate(); } catch {} }, 300);
}

function wsReconnect(ws) {
    wsSend(ws, { type: 'reconnect' });
    setTimeout(() => { try { ws.terminate(); } catch {} }, 300);
}

const PING_STR = JSON.stringify({ type: 'ping' });

function broadcast(obj, excludeWs = null) {
    const str = JSON.stringify(obj);
    for (const client of wss.clients) {
        if (client === excludeWs) continue;
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(str); } catch {}
        }
    }
}

function broadcastStaggered(pets, basePayload) {
    pets.forEach((pet, i) => {
        setTimeout(() => {
            broadcast({
                ...basePayload,
                name:     pet.name,
                gen:      pet.gen || '?',
                mutation: (pet.mutation && pet.mutation !== '' && pet.mutation !== 'None') ? pet.mutation : 'None',
                value:    pet.value || 0,
            });
        }, i * 150);
    });
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
    }).catch(() => {});
}

// ─── FIX 4: Clean embed builder ──────────────────
// Styled to match the KaWaifu reference screenshot:
// - Bold "Best" line with mutation tag
// - Clean bullet list for others
// - No "Buy a Slot!" text
// - Purple theme, thumbnail, clean footer
function buildEmbed(tierName, pets, jobInfo) {
    const best    = pets[0];
    const bestMut = (best.mutation && best.mutation !== 'None' && best.mutation !== '') ? best.mutation : 'Base';

    // Tier emoji
    const tierEmoji = tierName === 'Peaklights' ? '🏆' : tierName === 'Highlights' ? '⭐' : '💠';

    // Best line
    let description = `🥇 **Best**\n\`${bestMut}\` ${best.name} [**${displayGen(best.gen)}**]`;

    // Others
    if (pets.length > 1) {
        description += '\n\n🔹 **Others**';
        for (const pet of pets.slice(1)) {
            const mut = (pet.mutation && pet.mutation !== 'None' && pet.mutation !== '') ? pet.mutation : 'Base';
            description += `\n• \`${mut}\` ${pet.name} [${displayGen(pet.gen)}]`;
        }
    }

    return {
        title:       `${tierEmoji} Cerberus Notifier | ${tierName}`,
        description: description.slice(0, 3900),
        color:       0x9B59B6,
        thumbnail:   jobInfo.imageUrl ? { url: jobInfo.imageUrl } : undefined,
        fields: [
            {
                name:   '👥 Players',
                value:  jobInfo.players ? `${jobInfo.players}/8` : 'Unknown',
                inline: true,
            },
        ],
        footer:    { text: 'Cerberus Notifier • gg/cerberusnotifier', icon_url: LOGO_URL },
        timestamp: new Date().toISOString(),
    };
}

// ─── BOT TRACKING ────────────────────────────────
const botJobMap  = new Map();
const botCurrent = new Map();

function botJoin(userKey, jobId) {
    botLeave(userKey);
    botCurrent.set(userKey, jobId);
    if (!botJobMap.has(jobId)) botJobMap.set(jobId, new Set());
    botJobMap.get(jobId).add(userKey);
}

function botLeave(userKey) {
    const oldJob = botCurrent.get(userKey);
    if (!oldJob) return;
    botCurrent.delete(userKey);
    const set = botJobMap.get(oldJob);
    if (set) { set.delete(userKey); if (set.size === 0) botJobMap.delete(oldJob); }
}

function isJobOccupied(jobId) {
    const set = botJobMap.get(jobId);
    return set && set.size > 0;
}

async function getNextServerFromFetcher(requestingUserKey) {
    for (let i = 0; i < 10; i++) {
        let data;
        try {
            const res = await axios.get(`${FETCHER_URL}/next_server`, { timeout: 8_000 });
            data = res.data;
        } catch { return null; }
        if (!data || data.error || !data.job_id) return null;
        const jobId = data.job_id;
        if (!isJobOccupied(jobId)) return jobId;
        try { await axios.post(`${FETCHER_URL}/visited`, { job_id: jobId }, { timeout: 4_000 }); } catch {}
    }
    return null;
}

// ─── SCAN COORDINATION ───────────────────────────
const hopApproved = new Set();

app.post('/scan_done', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const { job_id, found_count, players } = req.body || {};
    if (!job_id) return res.status(400).json({ error: 'Missing job_id' });
    broadcast({ type: 'scan_result', job_id, found_count: found_count ?? 0, players: players ?? 0 });
    hopApproved.add(job_id);
    setTimeout(() => hopApproved.delete(job_id), 300_000);
    res.json({ ok: true });
});

app.get('/should_hop', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const jobId = (req.query.job_id || '').trim();
    if (!jobId) return res.status(400).json({ error: 'Missing job_id' });
    if (hopApproved.has(jobId)) { hopApproved.delete(jobId); return res.json({ hop: true }); }
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
    const { valid, user, reason } = await isKeyValid(userKey, 3);
    if (!valid) {
        console.log(`[Token] Rejected key ${userKey.slice(0,8)}… reason=${reason}`);
        return res.status(403).json({ error: 'Invalid or expired key', reason });
    }
    console.log('[Token] Issued for:', userKey.slice(0, 8) + '…');
    res.json({ token: generateToken(userKey, user) });
});

// ─── ACTIVE USER REGISTRY ────────────────────────
// Tracks roblox username+userId per userKey so AJ Users tab can display them
const activeUserInfo = new Map(); // userKey -> { username, userId, connectedAt }

app.post('/log_execute', async (req, res) => {
    const userKey = (req.headers['x-api-key'] || '').trim();
    if (!userKey) return res.status(401).json({ error: 'Unauthorized' });
    const { valid, user } = await isKeyValid(userKey, 3);
    if (!valid) return res.status(403).json({ error: 'Invalid or expired key' });
    const { username, userId } = req.body || {};
    if (!username) return res.status(400).json({ error: 'Missing username' });

    // Store for AJ Users tab
    activeUserInfo.set(userKey, {
        username: String(username),
        userId:   String(userId || ''),
        discordId: user?.discord_id || null,
        connectedAt: Math.floor(Date.now() / 1000),
    });

    const discordId  = user?.discord_id || null;
    const discordTag = discordId ? `<@${discordId}>` : 'Unknown';
    const webhook = process.env.WEBHOOK_EXECUTIONS;
    if (webhook) {
        fireWebhook(webhook, {
            title:  '🎮 Cerberus Execution',
            color:  0x9B59B6,
            fields: [
                { name: 'Roblox Username', value: String(username),      inline: true },
                { name: 'User ID',         value: String(userId || '?'), inline: true },
                { name: 'Discord',         value: discordTag,            inline: true },
            ],
        });
    }
    res.json({ ok: true });
});

// ─── AJ USERS ENDPOINT ───────────────────────────
// Returns active slot holders with roblox info + time remaining.
// Auth: valid user_key in query string (any connected user can poll this).
app.get('/aj_users', async (req, res) => {
    const userKey = (req.query.user_key || '').trim();
    if (!userKey) return res.status(401).json({ error: 'Missing user_key' });
    const { valid } = await isKeyValid(userKey, 1);
    if (!valid) return res.status(403).json({ error: 'Unauthorized' });

    const now   = Math.floor(Date.now() / 1000);
    const users = await getAllUsers(false); // use cache — this gets polled every 30s
    const active = users.filter(u =>
        !u.banned &&
        u.status !== 'banned' &&
        (u.auth_expire === -1 || u.auth_expire === 0 || u.auth_expire > now)
    );

    const result = active.map(u => {
        const info      = activeUserInfo.get(u.user_key);
        const secsLeft  = (u.auth_expire === -1 || u.auth_expire === 0)
            ? -1
            : Math.max(0, u.auth_expire - now);
        return {
            user_key:    u.user_key.slice(0, 8) + '…',
            discord_id:  u.discord_id || null,
            username:    info?.username || null,
            userId:      info?.userId   || null,
            secs_left:   secsLeft,
            note:        u.note || '',
        };
    });

    res.json({ ok: true, users: result });
});

app.post('/submit_batch', async (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const b = req.body;
    if (!b?.pets || !Array.isArray(b.pets) || b.pets.length === 0)
        return res.status(400).json({ error: 'Missing pets array' });
    const jobId = b.job_id || 'unknown';
    if (isServerRateLimited(jobId)) return res.status(429).json({ error: 'Rate limited' });
    const pets    = [...b.pets].sort((a, bb) => parseGen(bb.gen) - parseGen(a.gen));
    const best    = pets[0];
    const bestGen = parseGen(best.gen);
    broadcastStaggered(pets, { type:'brainrot', job_id:b.job_id||'', place_id:b.place_id||'' });
    res.json({ ok: true });

    let webhook  = null;
    let tierName = null;
    if      (bestGen >= 999e6) { webhook = process.env.WEBHOOK_999M_PLUS; tierName = 'Peaklights'; }
    else if (bestGen >= 400e6) { webhook = process.env.WEBHOOK_400_999M;  tierName = 'Highlights'; }
    else if (bestGen >= 50e6)  { webhook = process.env.WEBHOOK_50_400M;   tierName = 'Lowlights';  }

    if (webhook && tierName) {
        let imageUrl = b.image_url || null;
        if (!imageUrl) { for (const pet of pets) { imageUrl = await fetchWikiImage(pet.name); if (imageUrl) break; } }
        const embed = buildEmbed(tierName, pets, { imageUrl, players: b.players });
        fireWebhook(webhook, embed);
    }
});

app.post('/submit', async (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
    const b = req.body;
    if (!b?.name) return res.status(400).json({ error: 'Missing name' });
    const jobId = b.job_id || 'unknown';
    if (isServerRateLimited(jobId)) return res.status(429).json({ error: 'Rate limited' });
    broadcast({
        type:     'brainrot',
        name:     b.name,
        gen:      b.gen     || '?',
        mutation: (b.mutation && b.mutation !== '' && b.mutation !== 'None') ? b.mutation : 'None',
        value:    b.value   || 0,
        job_id:   b.job_id  || '',
        place_id: b.place_id || '',
    });
    const genVal = parseGen(b.gen);
    let webhook  = null;
    let tierName = null;
    if      (genVal >= 999e6) { webhook = process.env.WEBHOOK_999M_PLUS; tierName = 'Peaklights'; }
    else if (genVal >= 400e6) { webhook = process.env.WEBHOOK_400_999M;  tierName = 'Highlights'; }
    else if (genVal >= 50e6)  { webhook = process.env.WEBHOOK_50_400M;   tierName = 'Lowlights';  }

    if (webhook && tierName) {
        let imageUrl = b.image_url || null;
        if (!imageUrl) imageUrl = await fetchWikiImage(b.name);
        // Single pet — wrap in array for buildEmbed
        const embed = buildEmbed(tierName, [{
            name:     b.name,
            gen:      b.gen || '?',
            mutation: b.mutation || 'None',
        }], { imageUrl, players: b.players });
        fireWebhook(webhook, embed);
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

    const pathMatch = rawUrl.match(/^\/([a-f0-9]{64})(?:[/?]|$)/i);
    if (pathMatch) token = pathMatch[1];
    if (!token) {
        const qi = rawUrl.indexOf('?');
        if (qi >= 0) {
            try { token = new URLSearchParams(rawUrl.slice(qi + 1)).get('token') || null; } catch {}
        }
    }
    if (!token) {
        const proto = req.headers['sec-websocket-protocol'];
        if (proto) token = proto.split(',')[0].trim() || null;
    }
    if (!token) {
        const auth = req.headers['authorization'] || '';
        if (auth.startsWith('Bearer ')) token = auth.slice(7).trim() || null;
    }

    console.log('[WS] url=' + rawUrl.slice(0, 80) + ' token_found=' + !!token);

    if (!token) {
        try { ws.send(JSON.stringify({ type: 'reconnect' })); } catch {}
        ws.terminate(); return;
    }

    const entry = consumeToken(token);
    if (!entry) {
        try { ws.send(JSON.stringify({ type: 'expired' })); } catch {}
        ws.terminate(); return;
    }
    if (entry.notFound) {
        try { ws.send(JSON.stringify({ type: 'reconnect' })); } catch {}
        ws.terminate(); return;
    }

    const { userKey, user } = entry;
    ws._cerberusKey    = userKey;
    ws._authExpire     = user ? user.auth_expire : null;
    ws.isAlive         = true;
    ws._validFailCount = 0;

    console.log('[WS] Connected:', userKey.slice(0, 8) + '…');

    let expiryTimer = null;
    if (user && user.auth_expire !== -1 && user.auth_expire !== 0) {
        const secsLeft = user.auth_expire - Math.floor(Date.now() / 1000);
        if (secsLeft > 0) {
            expiryTimer = setTimeout(() => wsKick(ws), secsLeft * 1000);
        } else {
            wsKick(ws); return;
        }
    }

    let _username = null;
    let _jobId    = null;

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('error', () => {});

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
                        wsSend(ws, { type:'presence_join', username:existing, job_id:_jobId });
                    }
                }
            }
            presenceJoin(_jobId, _username);
            broadcast({ type:'presence_join', username:_username, job_id:_jobId }, ws);
            return;
        }
        broadcast(msg);
    });

    ws.on('close', (code) => {
        if (expiryTimer) clearTimeout(expiryTimer);
        botLeave(userKey);
        if (_username && _jobId) {
            presenceLeave(_jobId, _username);
            broadcast({ type:'presence_leave', username:_username, job_id:_jobId }, ws);
        }
        // Don't remove activeUserInfo on WS close — user may reconnect.
        // It gets naturally overwritten on next log_execute call.
        console.log('[WS] Closed key=' + userKey.slice(0,8) + '… code=' + code);
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

setInterval(() => {
    if (wss.clients.size === 0) return;
    const now = Math.floor(Date.now() / 1000);
    for (const ws of wss.clients) {
        if (ws.readyState !== WebSocket.OPEN) continue;
        if (ws._authExpire && ws._authExpire !== -1 && ws._authExpire !== 0 && ws._authExpire <= now) {
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
        const u = userMap.get(ws._cerberusKey);
        if (!u) {
            ws._validFailCount = (ws._validFailCount || 0) + 1;
            if (ws._validFailCount >= 3) wsKick(ws);
            continue;
        }
        const stillValid = !u.banned && (u.auth_expire === -1 || u.auth_expire === 0 || u.auth_expire > now);
        if (stillValid) {
            ws._validFailCount = 0;
            ws._authExpire = u.auth_expire;
        } else {
            ws._validFailCount = (ws._validFailCount || 0) + 1;
            if (ws._validFailCount >= 3) wsKick(ws);
        }
    }
}, 60_000);

setInterval(() => {
    for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) {
            try { client.send(PING_STR); } catch {}
        }
    }
}, 20_000);

function kickLiveSockets(userKey) {
    for (const client of wss.clients) {
        if (client._cerberusKey === userKey && client.readyState === WebSocket.OPEN) wsKick(client);
    }
}

async function rescheduleExpiryTimers() {
    const now   = Math.floor(Date.now() / 1000);
    const users = await getAllUsers(true);
    let   count = 0;
    for (const u of users) {
        if (u.banned || u.auth_expire === -1 || u.auth_expire === 0) continue;
        const secsLeft = u.auth_expire - now;
        if (secsLeft <= 0) continue;
        setTimeout(() => { kickLiveSockets(u.user_key); schedulePanel(500); }, secsLeft * 1000);
        count++;
    }
    console.log(`[Cerberus] ${count} expiry timer(s) scheduled`);
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
            console.warn(`[Discord] ${method} ${path} -> ${res.status}`);
            return null;
        }
        return res.json();
    } catch (err) {
        console.warn(`[Discord] ${method} ${path} threw:`, err.message);
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
    // Include 'reset' status — per Luarmor docs, reset means HWID was cleared but key is still valid
    const active = users.filter(u => !u.banned && u.status !== 'banned' && (u.auth_expire === -1 || u.auth_expire === 0 || u.auth_expire > now));
    const used   = active.length;
    const full   = used >= MAX_SLOTS;
    const lines  = active.length > 0
        ? active.map((u, i) => {
            const tag  = u.discord_id ? `<@${u.discord_id}>` : `\`${u.user_key.slice(0,8)}…\``;
            const time = (u.auth_expire === -1 || u.auth_expire === 0) ? '∞' : formatTime(u.auth_expire - now);
            return `${i+1}. ${tag} → ${time}`;
        }).join('\n')
        : '*No active slots.*';
    const embed = {
        title:       `🔴 Cerberus Notifier Active Slots (${used}/${MAX_SLOTS})`,
        description: lines + (full ? '\n\n**All slots are full at the moment.**' : ''),
        color:       0x9B59B6,
        thumbnail:   { url: LOGO_URL },
        footer:      { text: 'Cerberus Notifier • gg/cerberusnotifier', icon_url: LOGO_URL },
        timestamp:   new Date().toISOString(),
        fields: [{
            name:  full ? '⛔ All slots are full' : `✅ ${MAX_SLOTS-used} slot${MAX_SLOTS-used!==1?'s':''} available`,
            value: full ? 'Check back later or create a ticket for waitlist' : 'Create a ticket to purchase a slot',
            inline: false,
        }],
    };
    if (!panelMessageId) {
        const msg = await discordRequest('POST', `/channels/${CHANNEL_ID}/messages`, { embeds: [embed] });
        if (msg) panelMessageId = msg.id;
    } else {
        const result = await discordRequest('PATCH', `/channels/${CHANNEL_ID}/messages/${panelMessageId}`, { embeds: [embed] });
        if (!result) {
            panelMessageId = null;
            const msg = await discordRequest('POST', `/channels/${CHANNEL_ID}/messages`, { embeds: [embed] });
            if (msg) panelMessageId = msg.id;
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
        const mention=parts[1], durationStr=parts[2];
        if (!mention||!durationStr) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:'❌ Usage: `!addslot @user <duration>`'});
        const duration=parseDuration(durationStr);
        if (!duration||duration.seconds<7200) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:'❌ Minimum duration is 2h.'});
        const discordId=mention.replace(/[<@!>]/g,'');
        const now=Math.floor(Date.now()/1000);
        const users=await getAllUsers(true);
        const active=users.filter(u=>!u.banned&&(u.auth_expire===-1||u.auth_expire===0||u.auth_expire>now));
        if (active.length>=MAX_SLOTS) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:'❌ All slots are full!'});
        const existing=await getKeyByDiscordId(discordId);
        if (existing&&(existing.auth_expire===-1||existing.auth_expire===0||existing.auth_expire>now)) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:`❌ <@${discordId}> already has an active slot.`});
        const key=await createKey(duration.seconds,discordId,duration.label);
        if (!key) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:'❌ Failed to create key on Luarmor. Check server logs for details.'});
        setTimeout(()=>{kickLiveSockets(key);schedulePanel(500);},duration.seconds*1000);
        const dmChannel=await discordRequest('POST','/users/@me/channels',{recipient_id:discordId});
        if (dmChannel) {
            await discordRequest('POST',`/channels/${dmChannel.id}/messages`,{embeds:[{
                title:'🐕 Cerberus Notifier — Your Key',
                description:`Your slot is active for **${duration.label}**.\n\nHead to the 📡・finder-panel in the Discord server and redeem your key to get started.`,
                color:0x9B59B6, thumbnail:{url:LOGO_URL},
                fields:[{name:'🔑 Your Key',value:`\`${key}\``,inline:false},{name:'⏰ Duration',value:duration.label,inline:true}],
                footer:{text:'Cerberus Notifier • gg/cerberusnotifier'},
            }]});
        }
        await discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:`✅ Slot added for <@${discordId}> — **${duration.label}**. Key sent via DM.`});
        schedulePanel(500); return;
    }

    if (cmd === '!removeslot') {
        const mention=parts[1];
        if (!mention) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:'❌ Usage: `!removeslot @user`'});
        const discordId=mention.replace(/[<@!>]/g,'');
        const user=await getKeyByDiscordId(discordId);
        if (!user) return discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:`❌ No key found for <@${discordId}>.`});
        await revokeKey(user.user_key);
        kickLiveSockets(user.user_key);
        await discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:`✅ Slot removed for <@${discordId}> — kicked instantly.`});
        schedulePanel(500); return;
    }

    if (cmd === '!slots') {
        const now=Math.floor(Date.now()/1000);
        const users=await getAllUsers();
        const active=users.filter(u=>!u.banned&&(u.auth_expire===-1||u.auth_expire===0||u.auth_expire>now));
        await discordRequest('POST',`/channels/${msg.channel_id}/messages`,{content:`📊 Active slots: **${active.length}/${MAX_SLOTS}**`});
        return;
    }

    if (cmd === '!help') {
        await discordRequest('POST',`/channels/${msg.channel_id}/messages`,{embeds:[{
            title:'🐕 Cerberus Bot Commands', color:0x9B59B6,
            fields:[
                {name:'!addslot @user <duration>',value:'Add a slot. e.g. `2h` `1d` `1w` `1m`',inline:false},
                {name:'!removeslot @user',value:'Remove a slot — kicks live session instantly',inline:false},
                {name:'!slots',value:'Show active slot count',inline:false},
            ],
        }]});
    }
}

function startGateway() {
    gatewayWs = new WebSocket('wss://gateway.discord.gg/?v=10&encoding=json');
    gatewayWs.on('open', () => {});
    gatewayWs.on('message', async data => {
        let payload;
        try { payload = JSON.parse(data); } catch { return; }
        const { op, d, t, s } = payload;
        if (s) sequence = s;
        if (op === 10) {
            heartbeatGW = setInterval(() => {
                gatewayWs.send(JSON.stringify({ op:1, d:sequence }));
            }, d.heartbeat_interval);
            gatewayWs.send(JSON.stringify({
                op: 2,
                d: { token:BOT_TOKEN, intents:33280, properties:{os:'linux',browser:'cerberus',device:'cerberus'} },
            }));
        }
        if (op===0 && t==='READY') { console.log(`[Cerberus] Bot online: ${d.user.username}`); await rescheduleExpiryTimers(); updatePanel(); }
        if (op===0 && t==='MESSAGE_CREATE') await handleMessage(d);
    });
    gatewayWs.on('close', () => { clearInterval(heartbeatGW); setTimeout(startGateway, 5000); });
    gatewayWs.on('error', () => {});
}

startGateway();
setInterval(updatePanel, 60_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`[Cerberus] Running on port ${PORT}`));
