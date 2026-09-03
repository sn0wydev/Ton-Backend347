// ============================================
// TON PAYMENTS BACKEND
// ============================================
// Standalone service. Deploy separately from bot.js (this is what
// vgtserver-production.up.railway.app should actually run).
//
// Flow:
//   1. Client calls POST /ton/create-pending BEFORE sending the TON
//      transfer, with a productId (not a raw amount/stars — those are
//      looked up server-side so a tampered client can't set its own price).
//   2. Client sends the TON transfer via TonConnect (sendTransaction).
//   3. This service polls the merchant wallet's incoming transactions via
//      toncenter, matches on: sender address + amount (with small
//      tolerance) + reference in the transaction comment, within a time
//      window. On match, the pending payment is marked CONFIRMED and an
//      unclaimed Stars credit is recorded for that userId. Nothing is
//      trusted from the client's "transaction sent" status.
//   4. Client calls GET /ton/claim-credits/:userId (e.g. on syncBalance,
//      or right after sendPayment resolves) to pick up any confirmed-but-
//      undelivered credits. This endpoint marks them claimed and returns
//      the total. The client then calls its own Currency.addStars(total)
//      — CloudStorage can only be written from the client, so delivery
//      has to finish there, not in this service.
//
// Persistence is flat JSON files, matching the pattern already used by
// bot.js's payments.json. Swap for a real DB before serious volume —
// concurrent writes to a JSON file are not safe under load.
// ============================================

require('dotenv').config(); // loads .env in this same directory — see setup notes below

const express = require('express');
const cors = require('cors');
const fs = require('fs').promises;
const path = require('path');
const TelegramBot = require('node-telegram-bot-api');

// ============================================
// CONFIGURATION
// ============================================

const BOT_TOKEN            = process.env.BOT_TOKEN;                 // same bot as bot.js, for logging only — this service doesn't need bot.onText handlers
const MERCHANT_WALLET       = process.env.TON_MERCHANT_WALLET;       // your receiving wallet, raw or user-friendly form
const TONCENTER_API_KEY     = process.env.TONCENTER_API_KEY;         // get one at toncenter.com — unauthenticated calls are rate-limited hard
const TONCENTER_BASE        = process.env.TONCENTER_BASE || 'https://toncenter.com/api/v2';
const LOG_CHAT_ID           = process.env.LOG_CHAT_ID;
const TRANSACTION_LOG_TOPIC_ID = 3;
const HTTP_PORT             = process.env.PORT || 3001;

const POLL_INTERVAL_MS      = 12000;   // toncenter free tier: don't go much faster than this
const PENDING_EXPIRY_MS     = 30 * 60 * 1000;   // unmatched pending payments expire after 30 min
const AMOUNT_TOLERANCE_TON  = 0.0005;  // guards against float rounding, not fee differences (sender pays gas separately, doesn't touch this)

const DATA_DIR = __dirname;
const PENDING_FILE   = path.join(DATA_DIR, 'ton_pending.json');
const PROCESSED_FILE = path.join(DATA_DIR, 'ton_processed_tx.json');
const CREDITS_FILE   = path.join(DATA_DIR, 'ton_credits.json');

if (!MERCHANT_WALLET) { console.error('❌ TON_MERCHANT_WALLET is not set. Refusing to start.'); process.exit(1); }
if (!BOT_TOKEN)       { console.error('❌ BOT_TOKEN is not set. Refusing to start.'); process.exit(1); }

// Catches the easy-to-make mistake of pasting extra characters (a chat
// timestamp, trailing whitespace, a stray colon) along with the address.
// A friendly-form TON address is always exactly 48 base64url chars,
// optionally prefixed 0:/-1: if someone pasted the raw form instead.
// This won't validate the checksum, just the shape — good enough to stop
// an obviously-corrupted address from silently never matching anything.
const FRIENDLY_ADDR_RE = /^[A-Za-z0-9_-]{48}$/;
const RAW_ADDR_RE       = /^-?\d:[A-Fa-f0-9]{64}$/;
if (!FRIENDLY_ADDR_RE.test(MERCHANT_WALLET) && !RAW_ADDR_RE.test(MERCHANT_WALLET)) {
  console.error(`❌ TON_MERCHANT_WALLET doesn't look like a valid TON address: "${MERCHANT_WALLET}" (length ${MERCHANT_WALLET.length}). Check for extra pasted characters (e.g. a trailing timestamp). Refusing to start.`);
  process.exit(1);
}

// ============================================
// PRODUCT CATALOG — server is the source of truth.
// Must mirror DEPOSIT_PACKAGES.ton in script.js. If you change pricing
// there, change it here too — the client only ever sends a productId.
// ============================================

const TON_PACKAGES = {
  ton_tiny:   { amountTon: 0.5, stars: 200,   label: 'Tiny TON Package' },
  ton_small:  { amountTon: 1,   stars: 400,   label: 'Small TON Package' },
  ton_medium: { amountTon: 3,   stars: 1200,  label: 'Medium TON Package' },
  ton_large:  { amountTon: 5,   stars: 2000,  label: 'Large TON Package' },
  ton_xl:     { amountTon: 10,  stars: 4000,  label: 'XL TON Package' },
  ton_mega:   { amountTon: 25,  stars: 10000, label: 'Mega TON Package' }
};

// ============================================
// STATE (in-memory, mirrored to disk)
// ============================================

// reference -> { userId, productId, amountTon, stars, walletAddress, createdAt, status }
// status: 'pending' | 'confirmed' | 'expired'
let pendingPayments = new Map();

// Set of toncenter transaction ids ("lt:hash") already matched — prevents
// double-crediting if the poller sees the same tx across multiple runs.
let processedTx = new Set();

// userId -> [{ stars, reference, confirmedAt, claimed }]
let credits = new Map();

const bot = new TelegramBot(BOT_TOKEN, { polling: false });

// ============================================
// PERSISTENCE
// ============================================

async function loadJSON(file, fallback) {
  try {
    const raw = await fs.readFile(file, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function saveJSON(file, data) {
  try {
    await fs.writeFile(file, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error(`❌ Failed to write ${file}:`, err);
  }
}

async function loadState() {
  const pendingObj = await loadJSON(PENDING_FILE, {});
  pendingPayments = new Map(Object.entries(pendingObj));

  const processedArr = await loadJSON(PROCESSED_FILE, []);
  processedTx = new Set(processedArr);

  const creditsObj = await loadJSON(CREDITS_FILE, {});
  credits = new Map(Object.entries(creditsObj));

  console.log(`📂 Loaded state: ${pendingPayments.size} pending, ${processedTx.size} processed tx, ${credits.size} users with credit history`);
}

async function savePending()   { await saveJSON(PENDING_FILE, Object.fromEntries(pendingPayments)); }
async function saveProcessed() { await saveJSON(PROCESSED_FILE, Array.from(processedTx)); }
async function saveCredits()   { await saveJSON(CREDITS_FILE, Object.fromEntries(credits)); }

// ============================================
// LOGGING (mirrors bot.js style, same group/topic)
// ============================================

async function logTx(message) {
  if (!LOG_CHAT_ID) { console.log(message.replace(/<[^>]+>/g, '')); return; }
  try {
    await bot.sendMessage(LOG_CHAT_ID, message, {
      message_thread_id: TRANSACTION_LOG_TOPIC_ID,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    });
  } catch (err) {
    console.error('❌ Error sending log to Telegram:', err.message);
  }
}

// ============================================
// TONCENTER CLIENT
// ============================================

async function toncenterGet(endpoint, params) {
  const url = new URL(`${TONCENTER_BASE}/${endpoint}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));

  const headers = {};
  if (TONCENTER_API_KEY) headers['X-API-Key'] = TONCENTER_API_KEY;

  const res = await fetch(url.toString(), { headers });
  if (!res.ok) throw new Error(`toncenter ${endpoint} returned ${res.status}`);
  const data = await res.json();
  if (data.ok === false) throw new Error(`toncenter error: ${data.error || 'unknown'}`);
  return data.result;
}

// Fetches the most recent incoming transactions for the merchant wallet.
async function fetchRecentTransactions(limit = 30) {
  return toncenterGet('getTransactions', {
    address: MERCHANT_WALLET,
    limit: String(limit),
    archival: 'true'
  });
}

// nanotons string -> TON number
function nanoToTon(nano) {
  return Number(nano) / 1e9;
}

// ============================================
// CORE MATCHING LOGIC
// ============================================

let consecutivePollFailures = 0;
const POLL_FAILURE_ALERT_THRESHOLD = 5; // ~1 min at 12s interval — alert once, then every 5 more failures

async function pollMerchantWallet() {
  let txs;
  try {
    txs = await fetchRecentTransactions();
    if (consecutivePollFailures >= POLL_FAILURE_ALERT_THRESHOLD) {
      await logTx(`
✅ <b>TON POLLING RECOVERED</b>
━━━━━━━━━━━━━━━━━━━━

Toncenter requests are succeeding again after ${consecutivePollFailures} failed attempts.
📅 <b>Date:</b> ${new Date().toISOString()}
`);
    }
    consecutivePollFailures = 0;
  } catch (err) {
    consecutivePollFailures++;
    console.error(`❌ toncenter poll failed (${consecutivePollFailures} in a row):`, err.message);
    if (consecutivePollFailures === POLL_FAILURE_ALERT_THRESHOLD ||
        (consecutivePollFailures > POLL_FAILURE_ALERT_THRESHOLD && consecutivePollFailures % POLL_FAILURE_ALERT_THRESHOLD === 0)) {
      await logTx(`
🚨 <b>TON POLLING FAILING</b>
━━━━━━━━━━━━━━━━━━━━

${consecutivePollFailures} consecutive toncenter requests have failed.
❌ <b>Last error:</b> ${err.message}
🌐 <b>Endpoint:</b> <code>${TONCENTER_BASE}</code>
📅 <b>Date:</b> ${new Date().toISOString()}

<b>Impact:</b> No new payments are being detected right now. Check TONCENTER_API_KEY, TON_MERCHANT_WALLET, and toncenter status.
`);
    }
    return; // fail-safe: try again next interval, don't crash the loop
  }

  if (!Array.isArray(txs) || !txs.length) return;

  let dirtyPending = false;
  let dirtyProcessed = false;
  let dirtyCredits = false;

  for (const tx of txs) {
    const inMsg = tx.in_msg;
    if (!inMsg || !inMsg.source) continue; // outgoing tx or no sender — not a deposit

    const txId = `${tx.transaction_id?.lt}:${tx.transaction_id?.hash}`;
    if (processedTx.has(txId)) continue;

    const receivedTon = nanoToTon(inMsg.value || '0');
    const comment      = (inMsg.message || '').trim();
    const senderAddr   = inMsg.source;

    if (!comment) continue; // no reference in the comment, can't match

    const pending = pendingPayments.get(comment);
    if (!pending || pending.status !== 'pending') continue;

    // Expiry check
    if (Date.now() - pending.createdAt > PENDING_EXPIRY_MS) {
      pending.status = 'expired';
      dirtyPending = true;
      continue;
    }

    // Amount check
    if (receivedTon + AMOUNT_TOLERANCE_TON < pending.amountTon) {
      // Underpaid — don't match, don't consume. Leave pending in case a
      // second top-up transaction arrives, or let it expire naturally.
      continue;
    }

    // Sender address check — only enforced if the client reported one.
    // TonConnect gives you the connected wallet's address up front, so
    // this should normally be present and should normally match.
    if (pending.walletAddress && senderAddr && !addressesRoughlyMatch(pending.walletAddress, senderAddr)) {
      await logTx(`
⚠️ <b>TON PAYMENT SENDER MISMATCH</b>
━━━━━━━━━━━━━━━━━━━━

👤 <b>User ID:</b> <code>${pending.userId}</code>
🔖 <b>Reference:</b> <code>${comment}</code>
📥 <b>Expected sender:</b> <code>${pending.walletAddress}</code>
📤 <b>Actual sender:</b> <code>${senderAddr}</code>
💎 <b>Amount:</b> ${receivedTon} TON
📅 <b>Date:</b> ${new Date().toISOString()}

<b>Status:</b> NOT auto-credited — review manually
`);
      continue;
    }

    // Match confirmed.
    pending.status = 'confirmed';
    dirtyPending = true;

    processedTx.add(txId);
    dirtyProcessed = true;

    const userCredits = credits.get(String(pending.userId)) || [];
    userCredits.push({
      stars: pending.stars,
      reference: comment,
      confirmedAt: Date.now(),
      claimed: false
    });
    credits.set(String(pending.userId), userCredits);
    dirtyCredits = true;

    await logTx(`
✅ <b>TON PAYMENT CONFIRMED</b>
━━━━━━━━━━━━━━━━━━━━

👤 <b>User ID:</b> <code>${pending.userId}</code>
📦 <b>Product:</b> <code>${pending.productId}</code>
💎 <b>Amount:</b> ${receivedTon} TON
⭐ <b>Stars credited:</b> ${pending.stars}
🔖 <b>Reference:</b> <code>${comment}</code>
🔗 <b>Tx:</b> <code>${txId}</code>
📅 <b>Date:</b> ${new Date().toISOString()}

<b>Status:</b> Unclaimed — delivered when client next calls /ton/claim-credits
`);

    try {
      await bot.sendMessage(pending.userId,
        `✅ <b>TON payment confirmed!</b>\n\n${pending.stars} ⭐ Stars are ready — open the app to collect them.`,
        { parse_mode: 'HTML' }
      );
    } catch { /* user may have blocked the bot — non-fatal, they'll still see it via claim-credits */ }
  }

  // Sweep expired pendings that the loop above didn't touch this pass.
  // This is the closest thing to a "cancelled" state in this flow — the
  // client never sent (or we never saw) a matching on-chain transfer
  // within the window, so log it the same way a failed Stars payment
  // would be logged in bot.js.
  for (const [ref, p] of pendingPayments) {
    if (p.status === 'pending' && Date.now() - p.createdAt > PENDING_EXPIRY_MS) {
      p.status = 'expired';
      dirtyPending = true;

      await logTx(`
❌ <b>TON PAYMENT EXPIRED</b>
━━━━━━━━━━━━━━━━━━━━

👤 <b>User ID:</b> <code>${p.userId}</code>
📦 <b>Product:</b> <code>${p.productId}</code>
💎 <b>Expected:</b> ${p.amountTon} TON → ${p.stars} ⭐
🔖 <b>Reference:</b> <code>${ref}</code>
📅 <b>Date:</b> ${new Date().toISOString()}

<b>Status:</b> No matching on-chain transfer within ${PENDING_EXPIRY_MS / 60000} min — NO CHARGE MADE
`);
    }
  }

  if (dirtyPending)   await savePending();
  if (dirtyProcessed) await saveProcessed();
  if (dirtyCredits)   await saveCredits();
}

// Loose comparison — TonConnect and toncenter can report addresses in
// different formats (raw "0:abc..." vs user-friendly "EQ..."/"UQ...").
// This is intentionally forgiving: it strips the workchain prefix and
// compares the tail. Good enough to flag obvious mismatches without
// requiring a full address-parsing library; not a strict cryptographic check.
function addressesRoughlyMatch(a, b) {
  const norm = (s) => s.replace(/^[-\w]*:/, '').slice(-48).toLowerCase();
  return norm(a) === norm(b);
}

// ============================================
// EXPRESS APP
// ============================================

const app = express();
app.use(cors({ origin: '*', methods: ['GET', 'POST'], allowedHeaders: ['Content-Type'] }));
app.use(express.json());

app.get('/', (req, res) => {
  res.json({
    status: 'online',
    service: 'TON Payments Backend',
    pendingCount: pendingPayments.size,
    merchantWallet: MERCHANT_WALLET
  });
});

// Step 1: client calls this BEFORE sending the TON transfer.
app.post('/ton/create-pending', async (req, res) => {
  try {
    const { userId, productId, walletAddress } = req.body;

    if (!userId || !productId) {
      return res.status(400).json({ error: 'Missing userId or productId' });
    }

    const pkg = TON_PACKAGES[productId];
    if (!pkg) {
      return res.status(400).json({ error: 'Invalid productId', availableProducts: Object.keys(TON_PACKAGES) });
    }

    const reference = generateReference();

    pendingPayments.set(reference, {
      userId,
      productId,
      amountTon: pkg.amountTon,
      stars: pkg.stars,
      walletAddress: walletAddress || null,
      createdAt: Date.now(),
      status: 'pending'
    });
    await savePending();

    await logTx(`
⏳ <b>TON PAYMENT INITIATED</b>
━━━━━━━━━━━━━━━━━━━━

👤 <b>User ID:</b> <code>${userId}</code>
📦 <b>Product:</b> <code>${productId}</code>
💎 <b>Expected:</b> ${pkg.amountTon} TON → ${pkg.stars} ⭐
🔖 <b>Reference:</b> <code>${reference}</code>
📅 <b>Date:</b> ${new Date().toISOString()}

<b>Status:</b> Awaiting on-chain transfer
`);

    res.json({
      success: true,
      reference,
      amountTon: pkg.amountTon,
      stars: pkg.stars,
      merchantWallet: MERCHANT_WALLET
      // Client must put `reference` in the transaction COMMENT — that's
      // the only thing this service can match on. Plain address+amount
      // is not enough; multiple pending payments can share both.
    });
  } catch (err) {
    console.error('❌ /ton/create-pending error:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

// Optional: lets the client show "waiting for confirmation" / "confirmed"
// without polling claim-credits repeatedly.
app.get('/ton/status/:reference', (req, res) => {
  const pending = pendingPayments.get(req.params.reference);
  if (!pending) return res.status(404).json({ error: 'Unknown reference' });
  res.json({ status: pending.status, stars: pending.stars, amountTon: pending.amountTon });
});

// Step 2: client calls this to pick up confirmed-but-undelivered credits,
// then applies the returned total via its own Currency.addStars() so it
// gets written to CloudStorage from the client side.
app.get('/ton/claim-credits/:userId', async (req, res) => {
  try {
    const userId = String(req.params.userId);
    const userCredits = credits.get(userId) || [];
    const unclaimed = userCredits.filter(c => !c.claimed);

    if (!unclaimed.length) return res.json({ stars: 0, claimed: [] });

    const total = unclaimed.reduce((sum, c) => sum + c.stars, 0);
    unclaimed.forEach(c => { c.claimed = true; });
    credits.set(userId, userCredits);
    await saveCredits();

    await logTx(`
📬 <b>TON CREDITS CLAIMED</b>
━━━━━━━━━━━━━━━━━━━━

👤 <b>User ID:</b> <code>${userId}</code>
⭐ <b>Total delivered:</b> ${total}
🔖 <b>References:</b> ${unclaimed.map(c => `<code>${c.reference}</code>`).join(', ')}
📅 <b>Date:</b> ${new Date().toISOString()}
`);

    res.json({
      stars: total,
      claimed: unclaimed.map(c => ({ stars: c.stars, reference: c.reference }))
    });
  } catch (err) {
    console.error('❌ /ton/claim-credits error:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

function generateReference() {
  const four  = Math.floor(1000 + Math.random() * 9000);
  const two   = Math.floor(10 + Math.random() * 90);
  const three = Array.from({ length: 3 }, () =>
    String.fromCharCode(65 + Math.floor(Math.random() * 26))
  ).join('');
  return `TON-${four}-${two}-${three}`;
}

// ============================================
// STARTUP
// ============================================

async function start() {
  await loadState();

  app.listen(HTTP_PORT, () => {
    console.log(`🌐 TON Payments backend running on port ${HTTP_PORT}`);
  });

  pollMerchantWallet(); // run once immediately, don't wait for the first interval
  setInterval(pollMerchantWallet, POLL_INTERVAL_MS);
  console.log(`🔄 Polling ${MERCHANT_WALLET} every ${POLL_INTERVAL_MS / 1000}s`);
}

start().catch(err => {
  console.error('❌ Failed to start TON payments backend:', err);
  process.exit(1);
});