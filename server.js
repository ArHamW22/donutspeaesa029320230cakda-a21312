// ══════════════════════════════════════════════════
//  CERBERUS — COMBINED BACKEND + SLOTS BOT
// ══════════════════════════════════════════════════
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const crypto    = require('crypto');
const app       = express();
const server    = http.createServer(app);
const wss       = new WebSocket.Server({ server });
app.use(express.json({ limit: '10kb' }));

const API_KEY    = process.env.API_KEY;
const LRM_KEY    = process.env.LRM_KEY;
const LRM_PID    = process.env.LRM_PID;
const BOT_TOKEN  = process.env.BOT_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;
const MAX_SLOTS  = 7;
const LOGO_URL   = 'https://media.discordapp.net/attachments/1487763701040680971/1489669239202644089/image.png';

if (!API_KEY || !LRM_KEY || !LRM_PID || !BOT_TOKEN || !CHANNEL_ID) {
    console.error('Missing env vars: API_KEY, LRM_KEY, LRM_PID, BOT_TOKEN, CHANNEL_ID');
    process.exit(1);
}

app.get('/', (_req, res) => res.send('online'));

// ─── LUARMOR HELPERS ────────────────────────────
async function getAllUsers() {
    const res = await fetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
        headers: { 'Authorization': LRM_KEY, 'Content-Type': 'application/json' }
    });
    if (!res.ok) return [];
    const d = await res.json();
    return d.users || [];
}

async function createKey(durationSeconds, discordId, label) {
    const auth_expire = Math.floor(Date.now() / 1000) + durationSeconds;
    const res = await fetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users`, {
        method: 'POST',
        headers: { 'Authorization': LRM_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({ auth_expire, discord_id: discordId, note: `Cerberus — ${label}` })
    });
    const d = await res.json();
    if (!d.success) { console.log('[Luarmor] createKey failed:', d); return null; }
    return d.user_key;
}

async function revokeKey(userKey) {
    const res = await fetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(userKey)}`, {
        method: 'DELETE',
        headers: { 'Authorization': LRM_KEY, 'Content-Type': 'application/json' }
    });
    return res.ok;
}

async function getKeyByDiscordId(discordId) {
    const users = await getAllUsers();
    return users.find(u => u.discord_id === discordId) || null;
}

// ─── TOKEN SYSTEM ───────────────────────────────
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

// ─── KEY VALIDATION ─────────────────────────────
const invalidCache = new Map();

async function isKeyValid(key) {
    const cached = invalidCache.get(key);
    if (cached && Date.now() < cached) return false;
    try {
        const res = await fetch(`https://api.luarmor.net/v3/projects/${LRM_PID}/users?user_key=${encodeURIComponent(key)}`, {
            headers: { 'Authorization': LRM_KEY.trim(), 'Content-Type': 'application/json' }
        });
        if (!res.ok) { invalidCache.set(key, Date.now() + 10_000); return false; }
        const d = await res.json();
        if (!d.success || !d.users?.length) { invalidCache.set(key, Date.now() + 10_000); return false; }
        const u = d.users[0];
        if (u.banned) { invalidCache.set(key, Date.now() + 10_000); return false; }
        if (u.auth_expire !== -1 && u.auth_expire < Math.floor(Date.now() / 1000)) {
            invalidCache.set(key, Date.now() + 10_000); return false;
        }
        return true;
    } catch(e) { return false; }
}

// ─── ROUTES ─────────────────────────────────────
app.get('/get_token', async (req, res) => {
    const userKey = req.query.user_key;
    if (!userKey) return res.status(400).json({ error: 'Missing user_key' });
    const valid = await isKeyValid(userKey);
    if (!valid) return res.status(403).json({ error: 'Invalid or expired key' });
    res.json({ token: generateToken(userKey) });
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

// ─── BROADCAST ──────────────────────────────────
function broadcast(obj) {
    const buf = Buffer.from(JSON.stringify(obj));
    for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) client.send(buf);
    }
}

// ─── PRESENCE ───────────────────────────────────
const jobPresence = {};
const clientKeys  = new Map();

setInterval(async () => {
    for (const [ws, key] of clientKeys.entries()) {
        if (ws.readyState !== WebSocket.OPEN) { clientKeys.delete(ws); continue; }
        if (!await isKeyValid(key)) {
            try { ws.send(JSON.stringify({ type: 'expired' })); } catch(_) {}
            ws.close(4001, 'Key expired');
            clientKeys.delete(ws);
        }
    }
}, 10_000);

// ─── WEBSOCKET ──────────────────────────────────
wss.on('connection', async (ws, req) => {
    const rawUrl = req.url || '/';
    const qIndex = rawUrl.indexOf('?');
    const params = qIndex >= 0 ? new URLSearchParams(rawUrl.slice(qIndex + 1)) : new URLSearchParams();
    const token   = params.get('token');
    const userKey = token ? consumeToken(token) : null;

    if (!userKey) {
        try { ws.send(JSON.stringify({ type: 'expired' })); } catch(_) {}
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

// ─── PING ───────────────────────────────────────
setInterval(() => {
    const buf = Buffer.from(JSON.stringify({ type: 'ping' }));
    for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) client.send(buf);
    }
}, 20_000);

// ══════════════════════════════════════════════════
//  DISCORD BOT — ACTIVE SLOTS PANEL
// ══════════════════════════════════════════════════
let panelMessageId = null;
let sequence       = null;
let heartbeatInterval;
let gatewayWs;

function parseDuration(str) {
    const match = str.match(/^(\d+)(h|d|w|m)$/i);
    if (!match) return null;
    const num  = parseInt(match[1]);
    const unit = match[2].toLowerCase();
    if (unit === 'h') return { seconds: num * 3600,    label: `${num} hour${num !== 1 ? 's' : ''}` };
    if (unit === 'd') return { seconds: num * 86400,   label: `${num} day${num !== 1 ? 's' : ''}` };
    if (unit === 'w') return { seconds: num * 604800,  label: `${num} week${num !== 1 ? 's' : ''}` };
    if (unit === 'm') return { seconds: num * 2592000, label: `${num} month${num !== 1 ? 's' : ''}` };
    return null;
}

function formatTime(secs) {
    if (secs <= 0) return 'Expired';
    const d = Math.floor(secs / 86400);
    const h = Math.floor((secs % 86400) / 3600);
    const m = Math.floor((secs % 3600) / 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

async function discordRequest(method, path, body) {
    const res = await fetch(`https://discord.com/api/v10${path}`, {
        method,
        headers: { 'Authorization': `Bot ${BOT_TOKEN}`, 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined
    });
    if (!res.ok) {
        const t = await res.text();
        console.log(`[Discord] ${method} ${path} failed:`, t.slice(0, 200));
        return null;
    }
    return res.json();
}

async function updatePanel() {
    const now    = Math.floor(Date.now() / 1000);
    const users  = await getAllUsers();
    const active = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
    const used   = active.length;
    const full   = used >= MAX_SLOTS;

    const lines = active.length > 0
        ? active.map((u, i) => {
            const tag  = u.discord_id ? `<@${u.discord_id}>` : `\`${u.user_key.slice(0,8)}...\``;
            const time = u.auth_expire === -1 ? '∞' : formatTime(u.auth_expire - now);
            return `${i+1}. ${tag} → ${time}`;
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
            inline: false
        }]
    };

    if (!panelMessageId) {
        const msg = await discordRequest('POST', `/channels/${CHANNEL_ID}/messages`, { embeds: [embed] });
        if (msg) { panelMessageId = msg.id; console.log('[Panel] Posted:', panelMessageId); }
    } else {
        await discordRequest('PATCH', `/channels/${CHANNEL_ID}/messages/${panelMessageId}`, { embeds: [embed] });
        console.log('[Panel] Updated at', new Date().toLocaleTimeString());
    }
}

async function handleMessage(msg) {
    if (msg.author?.bot) return;
    const content = msg.content?.trim();
    if (!content?.startsWith('!')) return;
    const parts = content.split(' ');
    const cmd   = parts[0].toLowerCase();

    if (cmd === '!addslot') {
        const mention     = parts[1];
        const durationStr = parts[2];
        if (!mention || !durationStr) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Usage: `!addslot @user <duration>` e.g. `!addslot @user 1d` `!addslot @user 1w` `!addslot @user 1m`'
            });
        }
        const duration = parseDuration(durationStr);
        if (!duration || duration.seconds < 7200) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: '❌ Minimum duration is 2h. Use `2h`, `1d`, `1w`, `1m`'
            });
        }
        const discordId = mention.replace(/[<@!>]/g, '');
        const now       = Math.floor(Date.now() / 1000);
        const users     = await getAllUsers();
        const active    = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
        if (active.length >= MAX_SLOTS) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: '❌ All slots are full!' });
        }
        const existing = await getKeyByDiscordId(discordId);
        if (existing && (existing.auth_expire === -1 || existing.auth_expire > now)) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
                content: `❌ <@${discordId}> already has an active slot.`
            });
        }
        const key = await createKey(duration.seconds, discordId, duration.label);
        if (!key) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: '❌ Failed to create key on Luarmor.' });
        }
        const dmChannel = await discordRequest('POST', '/users/@me/channels', { recipient_id: discordId });
        if (dmChannel) {
            await discordRequest('POST', `/channels/${dmChannel.id}/messages`, {
                embeds: [{
                    title:       '🐕 Cerberus Notifier — Your Key',
                    description: `Your slot is active for **${duration.label}**.\n\nHead to the 📡・finder-panel in the Discord server and redeem your key to get started.`,
                    color:       0x00AF41,
                    thumbnail:   { url: LOGO_URL },
                    fields: [
                        { name: '🔑 Your Key',  value: `\`${key}\``, inline: false },
                        { name: '⏰ Duration',   value: duration.label, inline: true },
                    ],
                    footer: { text: 'Cerberus Notifier • gg/cerberusnotifier' }
                }]
            });
        }
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            content: `✅ Slot added for <@${discordId}> — **${duration.label}**. Key sent via DM.`
        });
        updatePanel();
    }

    if (cmd === '!removeslot') {
        const mention = parts[1];
        if (!mention) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: '❌ Usage: `!removeslot @user`' });
        }
        const discordId = mention.replace(/[<@!>]/g, '');
        const user      = await getKeyByDiscordId(discordId);
        if (!user) {
            return discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: `❌ No key found for <@${discordId}>.` });
        }
        await revokeKey(user.user_key);
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, { content: `✅ Slot removed for <@${discordId}>.` });
        updatePanel();
    }

    if (cmd === '!slots') {
        const now    = Math.floor(Date.now() / 1000);
        const users  = await getAllUsers();
        const active = users.filter(u => !u.banned && (u.auth_expire === -1 || u.auth_expire > now));
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            content: `📊 Active slots: **${active.length}/${MAX_SLOTS}**`
        });
    }

    if (cmd === '!help') {
        await discordRequest('POST', `/channels/${msg.channel_id}/messages`, {
            embeds: [{
                title: '🐕 Cerberus Bot Commands',
                color: 0x00AF41,
                fields: [
                    { name: '!addslot @user <duration>', value: 'Add a slot. e.g. `2h` `1d` `1w` `1m`', inline: false },
                    { name: '!removeslot @user',         value: 'Remove a slot',                        inline: false },
                    { name: '!slots',                    value: 'Show active slot count',               inline: false },
                ]
            }]
        });
    }
}

function startGateway() {
    gatewayWs = new WebSocket('wss://gateway.discord.gg/?v=10&encoding=json');
    gatewayWs.on('open', () => console.log('[Gateway] Connected'));
    gatewayWs.on('message', async (data) => {
        const payload = JSON.parse(data);
        const { op, d, t, s } = payload;
        if (s) sequence = s;
        if (op === 10) {
            heartbeatInterval = setInterval(() => {
                gatewayWs.send(JSON.stringify({ op: 1, d: sequence }));
            }, d.heartbeat_interval);
            gatewayWs.send(JSON.stringify({
                op: 2,
                d: { token: BOT_TOKEN, intents: 33280, properties: { os: 'linux', browser: 'cerberus', device: 'cerberus' } }
            }));
        }
        if (op === 0 && t === 'READY') {
            console.log('[Gateway] Bot ready:', d.user.username);
            updatePanel();
        }
        if (op === 0 && t === 'MESSAGE_CREATE') {
            await handleMessage(d);
        }
    });
    gatewayWs.on('close', (code) => {
        console.log('[Gateway] Closed:', code, '— reconnecting in 5s');
        clearInterval(heartbeatInterval);
        setTimeout(startGateway, 5000);
    });
    gatewayWs.on('error', (err) => console.log('[Gateway] Error:', err.message));
}

startGateway();
setInterval(updatePanel, 60_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Cerberus backend running on port', PORT));
