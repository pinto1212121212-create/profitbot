/**
 * ProfitBot Terminal — Cloudflare Worker v2
 * Webhook receiver + Tradovate API proxy + OSO brackets + Risk sizing + Trading windows
 * Deploy: Cloudflare Dashboard → Workers → Create → Paste → Deploy
 * Bind KV: Settings → Variables → KV Namespace Bindings → Add "PROFITBOT_KV"
 */
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Max-Age': '86400'
};
const TV = {
  demo: 'https://demo.tradovateapi.com/v1',
  live: 'https://live.tradovateapi.com/v1'
};
const CONTRACT_SPECS = {
  'MNQ': { tickSize: 0.25, tickValue: 0.5, pointValue: 2 },
  'NQ':  { tickSize: 0.25, tickValue: 5,   pointValue: 20 },
  'MES': { tickSize: 0.25, tickValue: 1.25,pointValue: 5 },
  'ES':  { tickSize: 0.25, tickValue: 12.5,pointValue: 50 },
  'MYM': { tickSize: 1,    tickValue: 0.5, pointValue: 0.5 },
  'YM':  { tickSize: 1,    tickValue: 5,   pointValue: 5 },
  'M2K': { tickSize: 0.1,  tickValue: 0.5, pointValue: 5 },
  'RTY': { tickSize: 0.1,  tickValue: 5,   pointValue: 50 },
  'CL':  { tickSize: 0.01, tickValue: 10,  pointValue: 1000 },
  'MCL': { tickSize: 0.01, tickValue: 1,   pointValue: 100 },
  'GC':  { tickSize: 0.1,  tickValue: 10,  pointValue: 100 },
  'MGC': { tickSize: 0.1,  tickValue: 1,   pointValue: 10 }
};
function getSpec(ticker) {
  const root = ticker.replace(/[A-Z]\d+$/, '').replace(/!$/, '');
  return CONTRACT_SPECS[root] || { tickSize: 0.01, tickValue: 1, pointValue: 1 };
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json', ...CORS }});
}

// ============ NOTIFICATION DISPATCHER ============
// Sends notifications to Discord / Telegram / Email based on saved config and event type
async function notifyChannels(kv, event, data) {
  const cfg = await kv.get('notif:channels', 'json');
  if (!cfg) return;
  
  // Determine which events are enabled
  const eventFilters = cfg.events || { signal: true, trade: true, fill: true, error: true, autoflat: true, alert: true };
  if (!eventFilters[event]) return;
  
  // Build message based on event type
  let title = '', body = '', emoji = '📡', level = 'info';
  switch (event) {
    case 'signal':
      emoji = '📡'; title = `Signal: ${data.action?.toUpperCase()} ${data.ticker}`;
      body = `Strategy: ${data.strategy}\nQty: ${data.qty}\nPrice: ${data.price || 'market'}`;
      break;
    case 'trade':
      emoji = data.pnl > 0 ? '✅' : data.pnl < 0 ? '❌' : '📊';
      title = `Trade ${data.pnl > 0 ? 'Profit' : 'Loss'}: ${data.ticker}`;
      body = `P&L: $${data.pnl?.toFixed(2)}\nStrategy: ${data.strategy}\nAction: ${data.action}`;
      break;
    case 'fill':
      emoji = '💰'; title = `Fill: ${data.action?.toUpperCase()} ${data.ticker}`;
      body = `Price: $${data.fillPrice}\nQty: ${data.qty}\nAccount: ${data.account}`;
      break;
    case 'error':
      emoji = '⚠️'; level = 'error'; title = `Error: ${data.action || 'unknown'} ${data.ticker || ''}`;
      body = `Reason: ${data.reason}\nStrategy: ${data.strategy || '-'}`;
      break;
    case 'autoflat':
      emoji = '🚨'; level = 'critical'; title = `Auto-Flat Executed`;
      body = `Reason: ${data.reason}\nClosed: ${data.totalClosed} positions\nFailed: ${data.totalFailed || 0}`;
      break;
    case 'alert':
      emoji = '🔔'; level = 'warn'; title = data.title || 'Alert';
      body = data.body || '';
      break;
  }
  
  // Discord
  if (cfg.discordEnabled && cfg.discordWebhookUrl) {
    try {
      const color = level === 'critical' ? 0xef4444 : level === 'error' ? 0xf59e0b : level === 'warn' ? 0xf59e0b : 0x4f7cff;
      await fetch(cfg.discordWebhookUrl, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ embeds: [{ title: emoji + ' ' + title, description: body, color, timestamp: new Date().toISOString(), footer: { text: 'ProfitBot Terminal' }}]})
      });
    } catch (e) { /* silent */ }
  }
  
  // Telegram
  if (cfg.telegramEnabled && cfg.telegramBotToken && cfg.telegramChatId) {
    try {
      const url = `https://api.telegram.org/bot${cfg.telegramBotToken}/sendMessage`;
      await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: cfg.telegramChatId, text: `${emoji} *${title}*\n\n${body}`, parse_mode: 'Markdown' })});
    } catch (e) { /* silent */ }
  }
  
  // Email (only for critical/error)
  if (cfg.emailEnabled && cfg.emailResendKey && cfg.emailTo && (level === 'critical' || level === 'error')) {
    try {
      await fetch('https://api.resend.com/emails', {
        method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg.emailResendKey },
        body: JSON.stringify({ from: cfg.emailFrom || 'onboarding@resend.dev', to: cfg.emailTo, subject: `${emoji} ${title}`, text: body })
      });
    } catch (e) { /* silent */ }
  }
}

async function tvAuth(env, creds) {
  // Tradovate requires cid+sec. If user didn't provide their own partner credentials,
  // fallback to Tradovate's public sample credentials (from their official example-api-csharp-trading repo).
  // These work for standard trading access on all accounts without the paid API Add-On.
  const body = {
    name: creds.username,
    password: creds.password,
    appId: 'ProfitBotTerminal',
    appVersion: '2.0',
    cid: creds.cid ? parseInt(creds.cid) : 8,
    sec: creds.sec || 'f03741b6-f634-48d6-9308-c8fb871150c2'
  };
  if (creds.deviceId) body.deviceId = creds.deviceId;
  const res = await fetch(TV[env] + '/auth/accessTokenRequest', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  });
  return res.json();
}
async function tvApi(env, path, token, method = 'GET', body = null, retries = 2, retryOpts = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token }};
  if (body) opts.body = JSON.stringify(body);
  // Phase 13: retryOpts = { initialDelayMs, multiplier, maxDelayMs } — from sub config
  const initialDelay = retryOpts?.initialDelayMs || 200;
  const multiplier = retryOpts?.multiplier || 2;
  const maxDelay = retryOpts?.maxDelayMs || 10000;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(TV[env] + path, opts);
      // Tradovate penalty system: back off on 429 / 5xx
      if ((res.status === 429 || res.status >= 500) && attempt < retries) {
        const backoffMs = Math.min(initialDelay * Math.pow(multiplier, attempt), maxDelay);
        await new Promise(r => setTimeout(r, backoffMs));
        continue;
      }
      let data;
      try { data = await res.json(); } catch { data = null; }
      return { ok: res.ok, status: res.status, data };
    } catch (err) {
      if (attempt === retries) return { ok: false, status: 0, data: null, error: String(err) };
      const backoffMs = Math.min(initialDelay * Math.pow(multiplier, attempt), maxDelay);
      await new Promise(r => setTimeout(r, backoffMs));
    }
  }
}
// Wrapper that auto-refreshes token on 401 and retries once
async function tvApiWithAuth(kv, conn, path, method = 'GET', body = null) {
  let token = await getCachedToken(kv, conn);
  if (!token) return { ok: false, status: 0, error: 'auth-failed' };
  let res = await tvApi(conn.env, path, token, method, body);
  // 401 = token invalid or expired. Force refresh and retry once.
  if (res.status === 401) {
    token = await getCachedToken(kv, conn, true); // forceRefresh
    if (!token) return { ok: false, status: 401, error: 'auth-refresh-failed' };
    res = await tvApi(conn.env, path, token, method, body);
  }
  return res;
}
async function getCachedToken(kv, conn, forceRefresh = false) {
  if (forceRefresh) await kv.delete(`token:${conn.id}`);
  let token = forceRefresh ? null : await kv.get(`token:${conn.id}`);
  if (!token) {
    const auth = await tvAuth(conn.env, conn);
    if (!auth.accessToken) return null;
    token = auth.accessToken;
    // Tradovate tokens are typically 80min; cache for 70min to be safe
    await kv.put(`token:${conn.id}`, token, { expirationTtl: 4200 });
  }
  return token;
}
// Comprehensive validator matching professional trading platform spec.
// Returns array of {code, message} for each validation issue.
function validatePayload(p) {
  const errors = [];
  const push = (code, message) => errors.push({ code, message });

  // Normalize aliases
  if (p.price !== undefined && p.signalPrice === undefined) p.signalPrice = p.price;
  if (p.sentiment) {
    const s = String(p.sentiment).toLowerCase();
    if (s === 'long') p.sentiment = 'bullish';
    else if (s === 'short') p.sentiment = 'bearish';
    else p.sentiment = s;
  }

  // ticker
  if (!p.ticker || typeof p.ticker !== 'string') push('empty-ticker-name', 'ticker required');

  // action
  const validActions = ['buy','sell','exit','cancel','add','scale_in','scale_out'];
  if (!p.action) push('invalid-action', 'action required');
  else if (!validActions.includes(String(p.action).toLowerCase())) push('invalid-action', `action must be one of: ${validActions.join(', ')}`);

  // sentiment (optional) — supports bullish/bearish/flat + long/short aliases
  if (p.sentiment !== undefined) {
    const validSentiments = ['bullish','bearish','flat'];
    if (!validSentiments.includes(String(p.sentiment).toLowerCase())) {
      push('invalid-sentiment', `sentiment must be one of: bullish, bearish, flat (long/short also accepted)`);
    }
    // sentiment only allowed with buy/sell actions per TradersPost spec
    const act = String(p.action || '').toLowerCase();
    if (!['buy','sell'].includes(act)) {
      push('invalid-sentiment-action', 'sentiment can only be used with action=buy or action=sell');
    }
  }

  // orderType
  if (p.orderType !== undefined) {
    const validTypes = ['market','limit','stop','stop_limit','trailing_stop'];
    if (!validTypes.includes(String(p.orderType).toLowerCase())) {
      push('invalid-order-type', `orderType must be one of: ${validTypes.join(', ')}`);
    }
  }
  // limit/stop price presence
  if ((p.orderType === 'limit' || p.orderType === 'stop_limit') && !p.limitPrice && !p.signalPrice) {
    push('invalid-order-type', 'limit order requires limitPrice or signalPrice');
  }
  if ((p.orderType === 'stop' || p.orderType === 'stop_limit') && !p.stopPrice && !p.signalPrice) {
    push('invalid-order-type', 'stop order requires stopPrice or signalPrice');
  }
  // trailing stop
  if (p.orderType === 'trailing_stop') {
    if (p.trailAmount !== undefined && p.trailPercent !== undefined) {
      push('invalid-trailing-stop', 'provide only one of trailAmount or trailPercent');
    }
    if (p.trailAmount === undefined && p.trailPercent === undefined) {
      push('invalid-trailing-stop', 'trailing_stop requires trailAmount or trailPercent');
    }
  }

  // cancelOrderType (filter for explicit cancel action)
  if (p.cancelOrderType !== undefined) {
    const validCancelTypes = ['market','limit','stop','stop_limit','trailing_stop','all'];
    if (!validCancelTypes.includes(String(p.cancelOrderType).toLowerCase())) {
      push('invalid-order-type', `cancelOrderType must be one of: ${validCancelTypes.join(', ')}`);
    }
  }

  // quantity / quantityType
  if (p.quantityType !== undefined) {
    const validQtyTypes = ['fixed_quantity','dollar_amount','amount_per_position','risk_dollar_amount','risk_per_position','risk_percent','percent_of_equity','percent_of_position'];
    if (!validQtyTypes.includes(String(p.quantityType))) {
      push('invalid-quantity-type', `quantityType must be one of: ${validQtyTypes.join(', ')}`);
    }
    if (p.quantity === undefined || p.quantity === null) {
      push('quantity-type-requires-quantity', 'quantityType requires quantity');
    }
    if ((p.quantityType === 'risk_dollar_amount' || p.quantityType === 'risk_per_position' || p.quantityType === 'risk_percent') && !p.stopLoss) {
      push('invalid-risk-dollar-amount-action', 'risk-based sizing requires stopLoss');
    }
    if (p.quantityType === 'percent_of_position' && !['add','exit','sell','buy'].includes(String(p.action).toLowerCase())) {
      push('invalid-percent-of-position-action', 'percent_of_position requires add/exit/sell/buy action');
    }
  }
  if (p.quantity !== undefined && (typeof p.quantity !== 'number' || p.quantity <= 0)) {
    push('invalid-quantity', 'quantity must be a positive number');
  }

  // takeProfit
  if (p.takeProfit) {
    const tp = p.takeProfit;
    const rel = [tp.percent, tp.amount].filter(v => v !== undefined).length;
    const abs = tp.limitPrice !== undefined ? 1 : 0;
    if (rel > 1) push('invalid-take-profit-amount-and-percent', 'takeProfit: provide only one of amount or percent');
    if (rel > 0 && abs > 0) push('invalid-take-profit-relative-and-absolute', 'takeProfit: provide either relative (amount/percent) or absolute (limitPrice)');
    if (rel === 0 && abs === 0) push('invalid-take-profit-value-required', 'takeProfit: provide amount, percent, or limitPrice');
    if (tp.percent !== undefined && (typeof tp.percent !== 'number' || tp.percent <= 0)) push('invalid-take-profit-percent', 'takeProfit.percent must be positive');
    if (tp.amount !== undefined && (typeof tp.amount !== 'number' || tp.amount <= 0)) push('invalid-take-profit-amount', 'takeProfit.amount must be positive');
    if (tp.limitPrice !== undefined && (typeof tp.limitPrice !== 'number' || tp.limitPrice <= 0)) push('invalid-take-profit-limit-price', 'takeProfit.limitPrice must be positive');
  }

  // stopLoss
  if (p.stopLoss) {
    const sl = p.stopLoss;
    const validSlTypes = ['stop','stop_limit','trailing_stop'];
    if (sl.type !== undefined && !validSlTypes.includes(String(sl.type))) {
      push('invalid-stop-loss-type', `stopLoss.type must be one of: ${validSlTypes.join(', ')}`);
    }
    const rel = [sl.percent, sl.amount].filter(v => v !== undefined).length;
    const abs = [sl.stopPrice, sl.limitPrice].filter(v => v !== undefined).length;
    const trail = [sl.trailAmount, sl.trailPercent].filter(v => v !== undefined).length;
    if (sl.type === 'trailing_stop') {
      if (trail === 0) push('invalid-stop-loss-trail-value-required', 'trailing_stop requires trailAmount or trailPercent');
      if (trail > 1) push('invalid-stop-loss-trail-value', 'provide only one of trailAmount or trailPercent');
    } else if (sl.type === 'stop_limit') {
      if (sl.stopPrice === undefined && rel === 0) push('invalid-stop-loss-stop-price-required', 'stop_limit requires stopPrice or amount/percent');
    } else {
      if (rel > 1) push('invalid-stop-loss-amount-and-percent', 'stopLoss: provide only one of amount or percent');
      if (rel > 0 && abs > 0) push('invalid-stop-loss-relative-and-absolute', 'stopLoss: provide either relative or absolute values');
      if (rel === 0 && abs === 0 && trail === 0) push('invalid-stop-loss-value-required', 'stopLoss: provide amount, percent, stopPrice, or trail values');
    }
    if (sl.percent !== undefined && (typeof sl.percent !== 'number' || sl.percent <= 0)) push('invalid-stop-loss-percent', 'stopLoss.percent must be positive');
    if (sl.amount !== undefined && (typeof sl.amount !== 'number' || sl.amount <= 0)) push('invalid-stop-loss-amount', 'stopLoss.amount must be positive');
    if (sl.stopPrice !== undefined && (typeof sl.stopPrice !== 'number' || sl.stopPrice <= 0)) push('invalid-stop-loss-stop-price', 'stopLoss.stopPrice must be positive');
  }

  // timeInForce
  if (p.timeInForce !== undefined) {
    const validTif = ['day','gtc','opg','cls','ioc','fok'];
    if (!validTif.includes(String(p.timeInForce).toLowerCase())) {
      push('invalid-time-in-force', `timeInForce must be one of: ${validTif.join(', ')}`);
    }
  }

  // time (ISO-8601)
  if (p.time !== undefined) {
    const d = new Date(p.time);
    if (isNaN(d.getTime())) push('invalid-payload', 'time must be valid ISO-8601 datetime');
  }

  // extendedHours
  if (p.extendedHours !== undefined && typeof p.extendedHours !== 'boolean') {
    push('invalid-extended-hours', 'extendedHours must be boolean');
  }

  return errors;
}
// DST-aware ET helpers. US DST: 2nd Sun of March → 1st Sun of November.
function wGetETOffsetHours(date = new Date()) {
  const year = date.getUTCFullYear();
  const march1 = new Date(Date.UTC(year, 2, 1));
  const marchSundayOffset = (7 - march1.getUTCDay()) % 7;
  const dstStart = new Date(Date.UTC(year, 2, 1 + marchSundayOffset + 7, 7, 0, 0));
  const nov1 = new Date(Date.UTC(year, 10, 1));
  const novSundayOffset = (7 - nov1.getUTCDay()) % 7;
  const dstEnd = new Date(Date.UTC(year, 10, 1 + novSundayOffset, 6, 0, 0));
  return (date >= dstStart && date < dstEnd) ? -4 : -5;
}
function wGetETNow(date = new Date()) {
  const et = new Date(date.getTime() + wGetETOffsetHours(date) * 3600 * 1000);
  return { hour: et.getUTCHours(), minute: et.getUTCMinutes(), totalMin: et.getUTCHours() * 60 + et.getUTCMinutes() };
}
function inTradingWindow(strategy) {
  if (!strategy.tradingWindow || !strategy.tradingWindow.enabled) return true;
  // Phase 13: honor tradingWindowTimezone (Asia/Jerusalem, ET, etc.)
  const tz = strategy.tradingWindowTimezone || 'America/New_York';
  let total;
  try {
    const now = new Date();
    const parts = new Intl.DateTimeFormat('en-US', { timeZone: tz, hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(now);
    const h = parseInt(parts.find(p => p.type === 'hour')?.value || '0');
    const m = parseInt(parts.find(p => p.type === 'minute')?.value || '0');
    total = h * 60 + m;
  } catch {
    total = wGetETNow().totalMin; // fallback to ET
  }
  const [sh, sm] = (strategy.tradingWindow.start || '00:00').split(':').map(Number);
  const [eh, em] = (strategy.tradingWindow.end || '23:59').split(':').map(Number);
  return total >= sh * 60 + sm && total <= eh * 60 + em;
}
function apexCheck(payload, strategy) {
  if (!strategy.apexMode) return { warnings: [], blocks: [] };
  const warnings = [], blocks = [];
  if (['buy','sell'].includes(payload.action.toLowerCase())) {
    if (!payload.stopLoss) blocks.push('APEX: entry requires stopLoss');
  }
  const et = wGetETNow();
  // Block after 16:55 ET (hard Apex cutoff — flat by 16:59)
  if (et.totalMin >= 16 * 60 + 55) blocks.push('APEX: past 16:55 ET');
  if (payload.action === 'add') warnings.push('APEX: verify not DCA on losing position');
  return { warnings, blocks };
}
async function checkRateLimit(kv, strategyId, strategy) {
  const now = Date.now();
  const cooldown = strategy.cooldownSeconds || 0;
  const rateLimit = strategy.maxPerHour || 0;
  if (cooldown > 0) {
    const last = await kv.get(`last:${strategyId}`);
    if (last && (now - parseInt(last)) < cooldown * 1000) {
      return { blocked: true, reason: 'cooldown' };
    }
  }
  if (rateLimit > 0) {
    const list = await kv.list({ prefix: `signal:${strategyId}:` });
    const hourStart = now - 3600000;
    const recent = list.keys.filter(k => parseInt(k.name.split(':')[2]) > hourStart).length;
    if (recent >= rateLimit) return { blocked: true, reason: 'rate-limit' };
  }
  await kv.put(`last:${strategyId}`, now.toString(), { expirationTtl: 86400 });
  return { blocked: false };
}

// ============ IF/THEN RULE ENGINE (Visual Strategy Builder) ============
// Evaluates a structured rule set against a signal payload + market context.
// Rule format: { operator: 'AND'|'OR', conditions: [{ field, op, value } | { type:'group', ... }] }
function wEvaluateRuleSet(ruleSet, payload, ctx) {
  if (!ruleSet || !ruleSet.conditions || !ruleSet.conditions.length) return { ok: true, matched: [] };
  return wEvaluateGroup(ruleSet, payload, ctx, []);
}
function wEvaluateGroup(group, payload, ctx, trail) {
  const op = (group.operator || 'AND').toUpperCase();
  const results = [];
  for (const c of group.conditions) {
    if (c.type === 'group') {
      const r = wEvaluateGroup(c, payload, ctx, trail);
      results.push(r.ok);
    } else {
      const r = wEvaluateCondition(c, payload, ctx);
      results.push(r);
      trail.push({ field: c.field, op: c.op, value: c.value, matched: r });
    }
  }
  const ok = op === 'AND' ? results.every(Boolean) : results.some(Boolean);
  return { ok, matched: trail };
}
function wEvaluateCondition(c, payload, ctx) {
  const val = wResolveField(c.field, payload, ctx);
  let target = c.value;
  if (typeof target === 'string' && !isNaN(parseFloat(target))) target = parseFloat(target);
  if (val === null || val === undefined || val === '') return false;
  switch (c.op) {
    case '<': return Number(val) < Number(target);
    case '<=': return Number(val) <= Number(target);
    case '>': return Number(val) > Number(target);
    case '>=': return Number(val) >= Number(target);
    case '==': case '=': return String(val) === String(target);
    case '!=': return String(val) !== String(target);
    case 'contains': return String(val).toLowerCase().includes(String(target).toLowerCase());
    case 'starts_with': return String(val).toLowerCase().startsWith(String(target).toLowerCase());
    case 'in': return Array.isArray(target) ? target.map(String).includes(String(val)) : String(target).split(',').map(s => s.trim()).includes(String(val));
    case 'between': {
      if (!Array.isArray(target) || target.length < 2) return false;
      const n = Number(val);
      return n >= Number(target[0]) && n <= Number(target[1]);
    }
    default: return false;
  }
}
function wResolveField(field, payload, ctx) {
  if (!field) return null;
  if (field.startsWith('payload.')) return payload[field.slice(8)];
  if (field.startsWith('indicator.')) return payload.indicators?.[field.slice(10)];
  if (field.startsWith('context.')) return ctx?.[field.slice(8)];
  switch (field) {
    case 'direction': return payload.action === 'buy' ? 'long' : payload.action === 'sell' ? 'short' : payload.action;
    case 'action': return payload.action;
    case 'ticker': return payload.ticker;
    case 'quantity': return payload.quantity;
    case 'price': case 'signalPrice': return payload.signalPrice || payload.limitPrice;
    case 'orderType': return payload.orderType;
    case 'hour': return ctx?.etHour;
    case 'minute': return ctx?.etMinute;
    case 'dayOfWeek': return ctx?.etDayOfWeek;
    case 'rsi': return payload.indicators?.rsi;
    case 'volume': return payload.indicators?.volume;
    case 'vwap': return payload.indicators?.vwap;
    case 'atr': return payload.indicators?.atr;
    case 'ema20': case 'ema50': case 'ema200':
      return payload.indicators?.[field];
    case 'spread': return ctx?.spread;
    case 'dailyPnl': return ctx?.dailyPnl;
    case 'dailyTrades': return ctx?.dailyTrades;
    default: return payload[field];
  }
}

// ============ ADVANCED STRATEGY FILTERS ============
// Pre-trade gate that checks custom user-defined conditions:
// - Day of week restrictions (Monday only, etc.)
// - Max daily profit lock (stop trading after hitting goal)
// - Max daily trade count
// - Custom IF/THEN rule set (visual builder output)
// - Account balance thresholds
async function checkStrategyFilters(kv, strategyId, strategy, payload) {
  const filters = strategy.filters || {};
  
  // 1. Day of Week filter
  if (filters.allowedDays && Array.isArray(filters.allowedDays) && filters.allowedDays.length > 0) {
    const etDate = wGetETNow();
    // 0=Sun, 1=Mon, ... 6=Sat — use ET day since markets follow ET
    const today = new Date();
    const etDayStr = today.toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long' }).toLowerCase();
    // allowedDays is array of lowercase day names: ["monday", "tuesday", ...]
    if (!filters.allowedDays.includes(etDayStr)) {
      return { blocked: true, reason: `Day filter: ${etDayStr} not allowed (allowed: ${filters.allowedDays.join(', ')})` };
    }
  }
  
  // 2. Max daily profit lock — if profit today exceeds target, block new entries
  // This "locks in" profits by preventing further risk after hitting goal
  if (filters.maxDailyProfit && filters.maxDailyProfit > 0) {
    const today = new Date().toISOString().slice(0, 10);
    const todayStart = new Date(today + 'T00:00:00Z').getTime();
    const tradesList = await kv.list({ prefix: 'trade:' + strategyId + ':', limit: 100 });
    let todayPnl = 0;
    for (const tk of tradesList.keys) {
      const t = await kv.get(tk.name, 'json');
      if (!t || t.status !== 'closed' || t.pnl === null) continue;
      const tradeTs = t.ts || t.timestamp || t.closedAt || 0;
      if (tradeTs < todayStart) continue;
      todayPnl += t.pnl;
    }
    if (todayPnl >= filters.maxDailyProfit) {
      return { blocked: true, reason: `Daily profit lock: $${todayPnl.toFixed(0)} >= target $${filters.maxDailyProfit}. No more trades today.` };
    }
  }
  
  // 3. Max daily trade count
  if (filters.maxDailyTrades && filters.maxDailyTrades > 0) {
    const today = new Date().toISOString().slice(0, 10);
    const todayStart = new Date(today + 'T00:00:00Z').getTime();
    const tradesList = await kv.list({ prefix: 'trade:' + strategyId + ':', limit: 100 });
    let todayCount = 0;
    for (const tk of tradesList.keys) {
      const t = await kv.get(tk.name, 'json');
      if (!t) continue;
      const tradeTs = t.ts || t.timestamp || 0;
      if (tradeTs < todayStart) continue;
      todayCount++;
    }
    if (todayCount >= filters.maxDailyTrades) {
      return { blocked: true, reason: `Daily trade limit: ${todayCount} >= ${filters.maxDailyTrades}` };
    }
  }
  
  // 4. Time of day restriction (minute-precision, ET)
  if (filters.noTradeBeforeET || filters.noTradeAfterET) {
    const et = wGetETNow();
    if (filters.noTradeBeforeET) {
      const [h, m] = filters.noTradeBeforeET.split(':').map(Number);
      if (et.totalMin < h * 60 + m) {
        return { blocked: true, reason: `Time filter: before ${filters.noTradeBeforeET} ET` };
      }
    }
    if (filters.noTradeAfterET) {
      const [h, m] = filters.noTradeAfterET.split(':').map(Number);
      if (et.totalMin > h * 60 + m) {
        return { blocked: true, reason: `Time filter: after ${filters.noTradeAfterET} ET` };
      }
    }
  }

  // 5. IF/THEN RULE SET (Visual Builder output)
  if (filters.ruleSet && filters.ruleSet.conditions && filters.ruleSet.conditions.length > 0) {
    const et = wGetETNow();
    // Build context for rule evaluation
    const today = new Date().toISOString().slice(0, 10);
    const todayStart = new Date(today + 'T00:00:00Z').getTime();
    let dailyPnl = 0, dailyTrades = 0;
    try {
      const tl = await kv.list({ prefix: 'trade:' + strategyId + ':', limit: 100 });
      for (const tk of tl.keys) {
        const t = await kv.get(tk.name, 'json');
        if (!t) continue;
        const ts = t.ts || t.timestamp || 0;
        if (ts < todayStart) continue;
        dailyTrades++;
        if (t.status === 'closed' && typeof t.pnl === 'number') dailyPnl += t.pnl;
      }
    } catch {}
    const nowD = new Date();
    const etDayOfWeek = (nowD.toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long' }).toLowerCase());
    const ctx = {
      etHour: et.hour, etMinute: et.minute, etDayOfWeek,
      dailyPnl, dailyTrades
    };
    const ruleResult = wEvaluateRuleSet(filters.ruleSet, payload, ctx);
    if (!ruleResult.ok) {
      const failedFields = ruleResult.matched.filter(m => !m.matched).map(m => `${m.field} ${m.op} ${m.value}`).slice(0, 3).join(', ');
      return { blocked: true, reason: `IF/THEN rule not met${failedFields ? ': ' + failedFields : ''}` };
    }
  }

  return { blocked: false };
}

function calcStopDistance(payload, entry) {
  if (!payload.stopLoss) return 0;
  const sl = payload.stopLoss;
  if (sl.amount) return sl.amount;
  if (sl.percent && entry) return entry * sl.percent / 100;
  if (sl.stopPrice && entry) return Math.abs(entry - sl.stopPrice);
  return 0;
}
async function calcQuantity(payload, env, token, account, subConfig) {
  const qtyType = payload.quantityType || subConfig?.quantityType || 'fixed_quantity';
  const qty = parseFloat(payload.quantity) || parseFloat(subConfig?.fixedQuantity) || 1;
  // Subscription-level scale factor: used for multi-account copy trading with different sizes
  // e.g. scaleFactor=2 doubles the calculated qty; scaleFactor=0.5 halves it
  const scaleFactor = parseFloat(subConfig?.scaleFactor) || 1;
  const applyScale = n => Math.max(1, Math.floor(n * scaleFactor));
  if (qtyType === 'fixed_quantity') return applyScale(qty);
  let equity = null, unrealized = 0;
  if (['risk_percent', 'percent_of_equity'].includes(qtyType)) {
    try {
      const cashRes = await tvApi(env, `/cashBalance/getcashbalancesnapshot`, token, 'POST', { accountId: account.id });
      equity = cashRes.data?.totalCashValue || cashRes.data?.amount || null;
      // Portfolio value minus unrealized for percent_of_equity (per TradersPost spec)
      try {
        const posRes = await tvApi(env, '/position/list', token);
        if (posRes.ok && Array.isArray(posRes.data)) {
          unrealized = posRes.data
            .filter(p => p.accountId === account.id)
            .reduce((s, p) => s + (p.netPos * (p.netPrice || 0) - (p.prevPrice || 0) * p.netPos), 0);
        }
      } catch {}
    } catch {}
  }
  const spec = getSpec(payload.ticker);
  const price = payload.signalPrice || payload.limitPrice;

  if (qtyType === 'dollar_amount' && price) return applyScale(qty / (price * spec.pointValue));

  // amount_per_position: quantity = amountPerPosition / entryPrice
  if (qtyType === 'amount_per_position' && price) {
    return applyScale(qty / (price * spec.pointValue));
  }

  if (qtyType === 'percent_of_equity' && equity != null && price) {
    // per TradersPost: portfolioValue = equity - unrealized
    const portfolioValue = equity - unrealized;
    const target = portfolioValue * (qty / 100);
    return applyScale(target / (price * spec.pointValue));
  }

  if (qtyType === 'risk_dollar_amount' && payload.stopLoss) {
    const slDist = calcStopDistance(payload, price);
    if (slDist > 0) return applyScale(qty / (slDist * spec.pointValue));
  }

  // risk_per_position: alias for risk_dollar_amount per TradersPost docs
  if (qtyType === 'risk_per_position' && payload.stopLoss) {
    const slDist = calcStopDistance(payload, price);
    if (slDist > 0) return applyScale(qty / (slDist * spec.pointValue));
  }

  if (qtyType === 'risk_percent' && equity && payload.stopLoss) {
    const risk = equity * (qty / 100);
    const slDist = calcStopDistance(payload, price);
    if (slDist > 0) return applyScale(risk / (slDist * spec.pointValue));
  }

  // percent_of_position: fetch current position and use a percentage of it
  if (qtyType === 'percent_of_position') {
    try {
      const posRes = await tvApi(env, '/position/list', token);
      if (posRes.ok && Array.isArray(posRes.data)) {
        const symUpper = String(payload.ticker || '').toUpperCase();
        const symRoot = symUpper.replace(/[A-Z]\d+$/, '').replace(/!$/, '');
        const pos = posRes.data.find(p => {
          const s = String(p.contractName || p.symbol || '').toUpperCase();
          return p.accountId === account.id && p.netPos !== 0 && (s === symUpper || s.startsWith(symRoot));
        });
        if (pos && pos.netPos) {
          return Math.max(1, Math.floor(Math.abs(pos.netPos) * (qty / 100)));
        }
      }
    } catch {}
    return applyScale(qty);
  }
  return applyScale(qty);
}
// Cancel all open (non-filled) orders for a specific symbol on an account.
// Optional cancelOrderType filter: 'market'|'limit'|'stop'|'stop_limit'|'trailing_stop'|'all'
// Returns {cancelled: number} - count of orders cancelled.
async function cancelOpenOrders(env, token, accountId, symbol, cancelOrderType = null) {
  try {
    const ordersRes = await tvApi(env, '/order/list', token);
    if (!ordersRes.ok || !Array.isArray(ordersRes.data)) return { cancelled: 0 };
    const symUpper = String(symbol || '').toUpperCase();
    const symRoot = symUpper.replace(/[A-Z]\d+$/, '').replace(/!$/, '');
    const typeMap = { 'market':'Market','limit':'Limit','stop':'Stop','stop_limit':'StopLimit','trailing_stop':'TrailingStop' };
    const filterType = cancelOrderType && cancelOrderType !== 'all' ? typeMap[cancelOrderType.toLowerCase()] : null;
    const openOrders = ordersRes.data.filter(o => {
      if (o.accountId !== accountId) return false;
      const s = String(o.contractName || o.symbol || '').toUpperCase();
      const matchSymbol = s === symUpper || s.startsWith(symRoot);
      const isOpen = ['Working','PendingNew','PendingReplace','PendingCancel','Suspended'].includes(o.ordStatus);
      const typeMatches = !filterType || o.orderType === filterType;
      return matchSymbol && isOpen && typeMatches;
    });
    let cancelled = 0;
    for (const o of openOrders) {
      try {
        const r = await tvApi(env, '/order/cancelorder', token, 'POST', { orderId: o.id });
        if (r.ok) cancelled++;
      } catch {}
    }
    return { cancelled };
  } catch (err) {
    return { cancelled: 0, error: String(err) };
  }
}

// Order Queueing: when broker rejects an order because market is closed,
// queue it for next market open. One queued trade per {sub, ticker}.
// Newer queued trades replace older ones for the same {sub, ticker}.
async function queueTrade(kv, subId, ticker, payload, strategyId, reason) {
  const queueKey = `queued:${subId}:${ticker}`;
  const entry = {
    subId, ticker, strategyId, payload, reason,
    queuedAt: Date.now(),
    expiresAt: Date.now() + (72 * 3600 * 1000) // 72 hour max queue life
  };
  await kv.put(queueKey, JSON.stringify(entry), { expirationTtl: 72 * 3600 });
  return entry;
}

// Check if broker response indicates "market closed" or similar queueable condition
function isQueueableFailure(orderRes) {
  if (!orderRes || orderRes.ok) return false;
  const reason = String(orderRes.data?.failureReason || '').toLowerCase();
  const text = String(orderRes.data?.failureText || '').toLowerCase();
  const combined = reason + ' ' + text;
  return /market.*closed|closed.*market|outside.*hours|session.*closed|exchange.*closed/.test(combined);
}

function buildOsoOrder(payload, account, qty, entryPrice) {
  const action = payload.action.toLowerCase() === 'sell' ? 'Sell' : 'Buy';
  const exitAction = action === 'Buy' ? 'Sell' : 'Buy';
  const orderType = (payload.orderType || 'market').toLowerCase();
  const tvType = { 'market': 'Market', 'limit': 'Limit', 'stop': 'Stop', 'stop_limit': 'StopLimit', 'trailing_stop': 'TrailingStop' }[orderType] || 'Market';
  const tifMap = { 'day': 'Day', 'gtc': 'GTC', 'opg': 'OPG', 'ioc': 'IOC', 'fok': 'FOK' };
  const order = {
    accountSpec: account.name, accountId: account.id, action,
    symbol: payload.ticker, orderQty: qty, orderType: tvType, isAutomated: true,
    timeInForce: tifMap[String(payload.timeInForce || '').toLowerCase()] || 'Day'
  };
  if (['limit', 'stop_limit'].includes(orderType)) order.price = payload.limitPrice;
  if (['stop', 'stop_limit'].includes(orderType)) order.stopPrice = payload.stopPrice;
  if (orderType === 'trailing_stop') {
    if (payload.trailAmount) order.trailAmount = payload.trailAmount;
    if (payload.trailPercent) order.trailPercent = payload.trailPercent;
  }
  let tpPrice = null, slPrice = null;
  const spec = getSpec(payload.ticker);
  if (payload.takeProfit && entryPrice) {
    const tp = payload.takeProfit;
    if (tp.limitPrice) tpPrice = tp.limitPrice;
    else if (tp.amount) tpPrice = entryPrice + (action === 'Buy' ? tp.amount : -tp.amount);
    else if (tp.percent) tpPrice = entryPrice * (1 + (action === 'Buy' ? tp.percent : -tp.percent) / 100);
    if (tpPrice) tpPrice = Math.round(tpPrice / spec.tickSize) * spec.tickSize;
  }
  if (payload.stopLoss && entryPrice) {
    const sl = payload.stopLoss;
    const slType = sl.type || 'stop';
    if (slType === 'trailing_stop') {
      // bracket stop as trailing — Tradovate supports Stop with trailAmount
      order.bracket1 = { action: exitAction, orderType: 'TrailingStop' };
      if (sl.trailAmount) order.bracket1.trailAmount = sl.trailAmount;
      if (sl.trailPercent) order.bracket1.trailPercent = sl.trailPercent;
      slPrice = null;
    } else {
      if (sl.stopPrice) slPrice = sl.stopPrice;
      else if (sl.amount) slPrice = entryPrice + (action === 'Buy' ? -sl.amount : sl.amount);
      else if (sl.percent) slPrice = entryPrice * (1 + (action === 'Buy' ? -sl.percent : sl.percent) / 100);
      if (slPrice) slPrice = Math.round(slPrice / spec.tickSize) * spec.tickSize;
    }
  }
  if (slPrice) order.bracket1 = { action: exitAction, orderType: 'Stop', stopPrice: slPrice };
  if (tpPrice) order.bracket2 = { action: exitAction, orderType: 'Limit', price: tpPrice };
  return { order, hasBrackets: !!(slPrice || tpPrice || order.bracket1), tpPrice, slPrice };
}
async function handleWebhook(request, env, strategyId, password) {
  if (request.method !== 'POST') return json({ error: 'post-required' }, 405);
  const kv = env.PROFITBOT_KV;
  if (!kv) return json({ error: 'KV not bound' }, 500);
  const strategy = await kv.get(`strategy:${strategyId}`, 'json');
  if (!strategy) return json({ error: 'strategy-not-found' }, 404);
  if (strategy.password !== password) return json({ success: false, messageCode: 'invalid-password', message: 'invalid webhook password' }, 403);
  let payload;
  try { payload = await request.json(); } catch (e) { return json({ success: false, messageCode: 'malformed-json', message: 'malformed JSON body' }, 400); }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return json({ success: false, messageCode: 'empty-json', message: 'request body must be a JSON object' }, 400);
  }

  // ========== IDEMPOTENCY GUARD ==========
  // Hash the payload + strategyId + current-minute bucket. If same hash seen in last 60s, reject as duplicate.
  // Skip guard if the client explicitly sends a unique _idempotencyKey or _approved flag.
  if (!payload._approved) {
    const keyBasis = payload._idempotencyKey ||
      `${strategyId}|${payload.ticker}|${payload.action}|${payload.quantity||1}|${payload.signalPrice||''}|${payload.orderType||'market'}|${Math.floor(Date.now()/60000)}`;
    const hashBuf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(keyBasis));
    const hashHex = Array.from(new Uint8Array(hashBuf)).map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
    const dupKey = `idem:${strategyId}:${hashHex}`;
    const existing = await kv.get(dupKey);
    if (existing) {
      await logSignal(kv, strategyId, payload, { status: 'duplicate', duplicateOf: existing, message: 'duplicate signal ignored (idempotency)' });
      return json({ success: false, messageCode: 'duplicate-signal', message: 'duplicate signal detected within 60s window', originalSignalId: existing }, 409);
    }
    // Mark this hash as seen, TTL 60s. We set after we compute a signal ID below.
    payload._idemDupKey = dupKey;
  }
  // =======================================

  const errors = validatePayload(payload);
  if (errors.length) {
    await logSignal(kv, strategyId, payload, { status: 'rejected', errors });
    return json({ success: false, messageCode: errors[0].code, message: errors[0].message, errors }, 400);
  }
  // Honor test flag: validate + log but don't submit orders
  if (payload.test === true) {
    await logSignal(kv, strategyId, payload, { status: 'test', test: true, message: 'test signal — no orders submitted' });
    return json({ success: true, test: true, message: 'test signal validated — no orders submitted' });
  }
  if (strategy.allowedTickers?.length) {
    const root = payload.ticker.replace(/[A-Z]\d+$/, '').replace(/!$/, '');
    if (!strategy.allowedTickers.includes(payload.ticker) && !strategy.allowedTickers.includes(root)) {
      await logSignal(kv, strategyId, payload, { status: 'rejected', errors: ['unsupported-ticker'] });
      return json({ error: 'unsupported-ticker' }, 400);
    }
  }
  if (!payload.ignoreTradingWindows && !inTradingWindow(strategy)) {
    await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'outside-trading-window', message: 'signal arrived outside configured trading window' }] });
    return json({ success: false, messageCode: 'outside-trading-window', message: 'signal outside trading window (use ignoreTradingWindows:true to override)' }, 400);
  }
  // Phase 13: manual signals bypass cooldown/rate-limit (source=manual from UI Submit Signal)
  const isManualOverride = payload.manualOverride === true || payload.source === 'manual';
  if (isManualOverride) {
    await logSignal(kv, strategyId, payload, { status: 'accepted-manual', reason: 'manual override — cooldown/rate-limit skipped' });
  } else {
    const rl = await checkRateLimit(kv, strategyId, strategy);
    if (rl.blocked) {
      await logSignal(kv, strategyId, payload, { status: 'rate-limited', reason: rl.reason });
      return json({ success: false, messageCode: 'too-many-requests', message: `rate-limited: ${rl.reason}`, details: rl }, 429);
    }
  }
  
  // ========== ADVANCED STRATEGY FILTERS ==========
  const filterResult = await checkStrategyFilters(kv, strategyId, strategy, payload);
  if (filterResult.blocked) {
    await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'strategy-filter', message: filterResult.reason }] });
    return json({ success: false, messageCode: 'strategy-filter', message: filterResult.reason }, 423);
  }

  // ========== STRATEGY-LEVEL SIDES CHECK (Long Only / Short Only / Both) ==========
  // TradersPost-style: block signals that don't match strategy-level `sides` setting
  const stratSides = strategy.sides || 'both';
  if (stratSides !== 'both') {
    const act = String(payload.action || '').toLowerCase();
    const isEntry = ['buy', 'sell'].includes(act);
    if (isEntry) {
      if (stratSides === 'long' && act === 'sell') {
        await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'strategy-sides', message: 'Strategy sides=Long Only rejects sell entry' }] });
        return json({ success: false, messageCode: 'strategy-sides', message: 'Strategy is Long Only — sell action blocked' }, 423);
      }
      if (stratSides === 'short' && act === 'buy') {
        await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'strategy-sides', message: 'Strategy sides=Short Only rejects buy entry' }] });
        return json({ success: false, messageCode: 'strategy-sides', message: 'Strategy is Short Only — buy action blocked' }, 423);
      }
    }
  }
  
  const compliance = apexCheck(payload, strategy);
  if (compliance.blocks.length) {
    await logSignal(kv, strategyId, payload, { status: 'blocked', errors: compliance.blocks.map(b => ({ code: 'compliance-blocked', message: b })) });
    return json({ success: false, messageCode: 'compliance-blocked', message: compliance.blocks[0], blocks: compliance.blocks }, 400);
  }
  
  // ========== DAILY LOSS LOCKDOWN CHECK ==========
  // Block NEW entries (buy/sell) if any subscription is in lockdown mode today.
  // Lockdowns are triggered automatically if daily loss exceeds conn.dailyLossLimit.
  const actionLower = String(payload.action || '').toLowerCase();
  const isNewEntry = ['buy', 'sell'].includes(actionLower);
  if (isNewEntry && strategy.subscriptions?.length) {
    const today = new Date().toISOString().slice(0, 10);
    for (const sub of strategy.subscriptions) {
      const lockKey = 'daily-lock:' + sub.connectionId + ':' + today;
      const lock = await kv.get(lockKey, 'json');
      if (lock) {
        await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'daily-loss-lockdown', message: `Connection ${sub.connectionId} locked: ${lock.reason}` }] });
        return json({ success: false, messageCode: 'daily-loss-lockdown', message: `connection ${sub.connectionId} locked: ${lock.reason}. Unlock via /api/daily-loss/${sub.connectionId}/unlock` }, 423);
      }
      // Also pro-actively compute daily P&L and auto-lock if exceeded limit
      const conn = await kv.get('connection:' + sub.connectionId, 'json');
      if (conn?.dailyLossLimit && conn.dailyLossLimit > 0) {
        // Use dedicated index for fast lookup: trade-by-conn:<connId>:
        const tradesList = await kv.list({ prefix: 'trade-by-conn:' + sub.connectionId + ':', limit: 200 });
        let dailyPnl = 0;
        const todayStart = new Date(today + 'T00:00:00Z').getTime();
        for (const tk of tradesList.keys) {
          const t = await kv.get(tk.name, 'json');
          if (!t) continue;
          // Use ts primarily, fall back to timestamp/closedAt for compatibility
          const tradeTs = t.ts || t.timestamp || t.closedAt || 0;
          if (tradeTs < todayStart) continue;
          // Only count CLOSED trades (with actual pnl)
          if (t.status !== 'closed' || t.pnl === null || t.pnl === undefined) continue;
          dailyPnl += t.pnl;
        }
        if (dailyPnl <= -Math.abs(conn.dailyLossLimit)) {
          // Auto-lock
          await kv.put(lockKey, JSON.stringify({ ts: Date.now(), reason: `Daily loss $${Math.abs(dailyPnl).toFixed(0)} exceeded limit $${conn.dailyLossLimit}`, dailyPnl }), { expirationTtl: 86400 });
          await kv.put('alert:daily-lock:' + Date.now(), JSON.stringify({ ts: Date.now(), connectionId: sub.connectionId, dailyPnl, limit: conn.dailyLossLimit }), { expirationTtl: 86400 * 7 });
          await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'daily-loss-auto-lock', message: `Auto-locked: daily loss $${Math.abs(dailyPnl).toFixed(0)} exceeded limit $${conn.dailyLossLimit}` }] });
          return json({ success: false, messageCode: 'daily-loss-auto-lock', message: `connection auto-locked: daily loss $${Math.abs(dailyPnl).toFixed(0)} exceeded limit $${conn.dailyLossLimit}`, dailyPnl, limit: conn.dailyLossLimit }, 423);
        }
      }
    }
  }
  // ==============================================
  
  // ========== CONSECUTIVE LOSSES AUTO-PAUSE ==========
  // Prevent revenge trading: if N losing trades in a row, pause strategy temporarily
  // Configurable per strategy: strategy.maxConsecutiveLosses (default: 3)
  if (isNewEntry && strategy.maxConsecutiveLosses && strategy.maxConsecutiveLosses > 0) {
    // Use strategy-specific prefix for efficient lookup
    const tradesList = await kv.list({ prefix: 'trade:' + strategyId + ':', limit: 100 });
    const stratTrades = [];
    for (const tk of tradesList.keys) {
      const t = await kv.get(tk.name, 'json');
      // Only count CLOSED trades with actual pnl
      if (!t || t.status !== 'closed' || t.pnl === null || t.pnl === undefined) continue;
      stratTrades.push(t);
    }
    // Sort by closedAt (fall back to ts) descending — most recent first
    stratTrades.sort((a, b) => (b.closedAt || b.ts || 0) - (a.closedAt || a.ts || 0));
    let consecLosses = 0;
    for (const t of stratTrades.slice(0, strategy.maxConsecutiveLosses + 1)) {
      if (t.pnl < 0) consecLosses++;
      else break;
    }
    if (consecLosses >= strategy.maxConsecutiveLosses) {
      // Check if there's a recent unpause
      const pauseKey = 'consec-pause:' + strategyId;
      const pause = await kv.get(pauseKey, 'json');
      const pauseAge = pause ? Date.now() - pause.ts : Infinity;
      // Auto-unpause after the configured cooldown (default 60 min)
      const cooldownMs = (strategy.consecPauseCooldownMin || 60) * 60000;
      if (pauseAge < cooldownMs) {
        await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'consec-losses-pause', message: `Paused after ${consecLosses} consecutive losses. Cooldown: ${Math.ceil((cooldownMs - pauseAge) / 60000)} min left.` }] });
        return json({ success: false, messageCode: 'consec-losses-pause', message: `strategy paused: ${consecLosses} consecutive losses. Wait ${Math.ceil((cooldownMs - pauseAge) / 60000)} min or unpause manually.`, consecLosses }, 423);
      }
      // First time hitting threshold — set pause
      if (!pause || pauseAge >= cooldownMs) {
        await kv.put(pauseKey, JSON.stringify({ ts: Date.now(), consecLosses, reason: `${consecLosses} consecutive losses` }), { expirationTtl: Math.ceil(cooldownMs / 1000) + 60 });
        await kv.put('alert:consec-pause:' + Date.now(), JSON.stringify({ ts: Date.now(), strategyId, consecLosses }), { expirationTtl: 86400 * 7 });
        await logSignal(kv, strategyId, payload, { status: 'blocked', errors: [{ code: 'consec-losses-first-trigger', message: `First trigger: ${consecLosses} consecutive losses. Cooldown started.` }] });
        return json({ success: false, messageCode: 'consec-losses-triggered', message: `strategy auto-paused: ${consecLosses} consecutive losses. Cooldown: ${strategy.consecPauseCooldownMin || 60} minutes.`, consecLosses }, 423);
      }
    }
  }
  // ==============================================
  
  if (payload.delay > 0) {
    await new Promise(r => setTimeout(r, Math.min(payload.delay * 1000, 30000)));
  }
  if (strategy.paperMode) {
    // Simulate paper fill with configured Market Price Type
    // Options: 'midpoint' (default), 'ask_for_buys_bid_for_sells', 'always_ask', 'always_bid', 'last'
    const priceType = strategy.paperPriceType || 'midpoint';
    const action = String(payload.action || '').toLowerCase();
    let simulatedFillPrice = payload.signalPrice || payload.limitPrice || payload.stopPrice || 0;

    // Tiny synthetic spread (0.05%) so midpoint/bid/ask differ for simulation realism
    if (simulatedFillPrice) {
      const syntheticSpread = simulatedFillPrice * 0.0005;
      const bid = simulatedFillPrice - syntheticSpread;
      const ask = simulatedFillPrice + syntheticSpread;
      if (priceType === 'always_bid') simulatedFillPrice = bid;
      else if (priceType === 'always_ask') simulatedFillPrice = ask;
      else if (priceType === 'ask_for_buys_bid_for_sells') simulatedFillPrice = action === 'buy' ? ask : bid;
      else if (priceType === 'last') simulatedFillPrice = (bid + ask) / 2;
      // midpoint (default) keeps signalPrice unchanged
    }

    const sim = {
      status: 'paper', ticker: payload.ticker, action: payload.action,
      qty: payload.quantity || 1, ts: Date.now(),
      fillPrice: simulatedFillPrice, priceType
    };
    const sigId = await logSignal(kv, strategyId, payload, { status: 'processed', paper: true, simulated: sim });
    await logTrade(kv, strategyId, 'paper', payload, { orderId: 'PAPER-' + Date.now(), fillPrice: simulatedFillPrice }, true, {
      entryPrice: simulatedFillPrice, qty: payload.quantity || 1
    });
    return json({ success: true, paper: true, simulated: sim, id: sigId, logId: sigId });
  }
  const subs = strategy.subscriptions || [];
  const results = [];
  const pendingApprovals = [];
  
  // ========== BROKER FAILOVER CHECK ==========
  // If failover is configured and primary connection is unhealthy, swap to backup
  const failoverCfg = await kv.get('failover:config', 'json');
  if (failoverCfg?.enabled && failoverCfg.primaryConnectionId && failoverCfg.backupConnectionId) {
    const primaryHealth = await kv.get('health:' + failoverCfg.primaryConnectionId, 'json');
    const primaryDown = !primaryHealth || 
                        (Date.now() - primaryHealth.ts > 600000) || // stale > 10min
                        !['healthy', 'slow'].includes(primaryHealth.status);
    if (primaryDown) {
      // Swap: replace primary with backup in all subs
      let swapped = 0;
      for (const sub of subs) {
        if (sub.connectionId === failoverCfg.primaryConnectionId) {
          sub.connectionId = failoverCfg.backupConnectionId;
          sub._failoverActive = true;
          swapped++;
        }
      }
      if (swapped > 0) {
        await kv.put('alert:failover-active:' + Date.now(), JSON.stringify({ 
          ts: Date.now(), primary: failoverCfg.primaryConnectionId, 
          backup: failoverCfg.backupConnectionId, swapped,
          reason: primaryHealth?.status || 'stale'
        }), { expirationTtl: 86400 * 7 });
        notifyChannels(kv, 'alert', { 
          title: '🔄 Broker Failover Active', 
          body: `Primary broker down (${primaryHealth?.status || 'stale'}). Switched ${swapped} subscriptions to backup.` 
        }).catch(() => {});
      }
    }
  }
  
  for (const sub of subs) {
    if (!sub.enabled) { results.push({ sub: sub.id, skipped: 'disabled' }); continue; }

    // Phase 13: per-sub ignoreExits check
    const act = String(payload.action || '').toLowerCase();
    if (sub.ignoreExits && (act === 'exit' || act === 'cancel')) {
      results.push({ sub: sub.id, skipped: 'ignoreExits=true' });
      continue;
    }

    // Phase 13: per-sub sides filter (overrides strategy-level)
    if (sub.sides && sub.sides !== 'both' && ['buy','sell'].includes(act)) {
      const isLong = act === 'buy' || (payload.sentiment === 'long' || payload.sentiment === 'bullish');
      const isShort = act === 'sell' || (payload.sentiment === 'short' || payload.sentiment === 'bearish');
      if (sub.sides === 'long' && isShort) {
        if (!sub.allowSideSwap) { results.push({ sub: sub.id, skipped: 'sides=long-only' }); continue; }
      }
      if (sub.sides === 'short' && isLong) {
        if (!sub.allowSideSwap) { results.push({ sub: sub.id, skipped: 'sides=short-only' }); continue; }
      }
    }

    const conn = await kv.get(`connection:${sub.connectionId}`, 'json');
    if (!conn) { results.push({ sub: sub.id, error: 'connection-not-found' }); continue; }
    if (conn.paused) { results.push({ sub: sub.id, skipped: 'connection-paused' }); continue; }

    // Require-approval gate: queue a pending approval, don't execute.
    // Client-side approval flow re-submits the signal with _approved=true.
    if (sub.requireApproval && !payload._approved) {
      const approvalId = crypto.randomUUID();
      const approval = {
        id: approvalId,
        strategyId, subscriptionId: sub.id, connectionId: sub.connectionId,
        accountId: sub.accountId,
        payload, status: 'pending',
        timestamp: Date.now(),
        expiresAt: Date.now() + 30 * 60 * 1000 // 30min expiry
      };
      pendingApprovals.push(approval);
      await kv.put(`approval:${approvalId}`, JSON.stringify(approval), { expirationTtl: 3600 });
      results.push({ sub: sub.id, pending: true, approvalId });
      continue;
    }

    const token = await getCachedToken(kv, conn);
    if (!token) { results.push({ sub: sub.id, error: 'auth-failed' }); continue; }
    let entryPrice = payload.signalPrice || payload.limitPrice || payload.stopPrice;
    // Phase 13: fetch quote for bid/ask-based entry price types
    const entryPT = sub.entryPriceType || 'default';
    const needsQuote = !entryPrice || (entryPT !== 'default' && ['buy','sell'].includes(payload.action.toLowerCase()));
    let quoteData = null;
    if (needsQuote && ['buy','sell'].includes(payload.action.toLowerCase())) {
      try {
        const ctrRes = await tvApi(conn.env, `/contract/suggest?t=${payload.ticker}&l=1`, token);
        if (ctrRes.ok && ctrRes.data?.[0]) {
          const qr = await tvApi(conn.env, `/md/getquote?contractId=${ctrRes.data[0].id}`, token);
          if (qr.ok) {
            quoteData = qr.data;
            if (!entryPrice) entryPrice = qr.data?.last || qr.data?.bid || null;
          }
        }
      } catch {}
    }
    // Phase 13: apply entryPriceType logic (Bid/Ask/Midpoint) — overrides if quote available
    if (quoteData && entryPT !== 'default') {
      const bid = quoteData.bid || quoteData.bidPrice;
      const ask = quoteData.ask || quoteData.askPrice;
      const isBuy = payload.action.toLowerCase() === 'buy';
      if (entryPT === 'bid_ask') entryPrice = isBuy ? (ask || entryPrice) : (bid || entryPrice);
      else if (entryPT === 'midpoint' && bid && ask) entryPrice = (bid + ask) / 2;
      else if (entryPT === 'ask' && ask) entryPrice = ask;
      else if (entryPT === 'bid' && bid) entryPrice = bid;
    }
    // Signal → Subscription inheritance: apply overrides. "Allow signal override" is controlled per field.
    // If sub.allowSignalOverride[field] === false, ignore signal value and use sub default.
    const effectivePayload = { ...payload };
    const allowOv = sub.allowSignalOverride || {}; // default: all true (inherit signal)
    // Phase 13: unified entry config fallback chain — entryOrderType (new) → defaultOrderType (legacy)
    const subEntryOT = sub.entryOrderType || sub.defaultOrderType;
    const subEntryTIF = sub.entryTIF || sub.defaultTimeInForce;
    if (allowOv.quantity === false && sub.fixedQuantity) effectivePayload.quantity = sub.fixedQuantity;
    if (allowOv.orderType === false && subEntryOT) effectivePayload.orderType = subEntryOT;
    if (allowOv.stopLoss === false && sub.defaultStopLoss) effectivePayload.stopLoss = sub.defaultStopLoss;
    if (allowOv.takeProfit === false && sub.defaultTakeProfit) effectivePayload.takeProfit = sub.defaultTakeProfit;
    if (allowOv.timeInForce === false && subEntryTIF) effectivePayload.timeInForce = subEntryTIF;
    // Phase 13: if no explicit orderType in signal, fallback to sub's entry config (even if override allowed)
    if (!effectivePayload.orderType && subEntryOT && subEntryOT !== 'default') effectivePayload.orderType = subEntryOT;
    if (!effectivePayload.timeInForce && subEntryTIF && subEntryTIF !== 'default') effectivePayload.timeInForce = subEntryTIF;
    // Phase 13: Position Size method — sub.quantityMethod overrides quantity
    if (!effectivePayload.quantity && sub.quantityMethod && sub.quantityMethod !== 'none') {
      if (sub.quantityMethod === 'fixed' && sub.quantityValue) effectivePayload.quantity = sub.quantityValue;
      else if (sub.quantityMethod === 'risk_dollar' && sub.riskDollar) { effectivePayload.quantityType = 'risk_dollar_amount'; effectivePayload.quantity = sub.riskDollar; }
      else if (sub.quantityMethod === 'risk_percent' && sub.riskPercent) { effectivePayload.quantityType = 'risk_percent'; effectivePayload.quantity = sub.riskPercent; }
      else if (sub.quantityMethod === 'amount' && sub.amount) { effectivePayload.quantityType = 'dollar_amount'; effectivePayload.quantity = sub.amount; }
    }
    // Phase 13: signal quantity multiplier (scales signal.quantity by factor)
    if (sub.signalQuantityMultiplier && effectivePayload.quantity) {
      effectivePayload.quantity = effectivePayload.quantity * sub.signalQuantityMultiplier;
      if (!sub.useFractionalQuantity) effectivePayload.quantity = Math.max(1, Math.floor(effectivePayload.quantity));
    }
    // Apply subscription SL/TP overrides (in ticks) — these override absolute amounts
    if (sub.slOverride && effectivePayload.stopLoss) {
      effectivePayload.stopLoss = { ...effectivePayload.stopLoss, amount: sub.slOverride * (getSpec(payload.ticker).tickSize) };
    }
    if (sub.tpOverride && effectivePayload.takeProfit) {
      effectivePayload.takeProfit = { ...effectivePayload.takeProfit, amount: sub.tpOverride * (getSpec(payload.ticker).tickSize) };
    }

    // Explicit cancel action — cancel pending orders (optionally filtered by type)
    if (payload.action.toLowerCase() === 'cancel') {
      // Phase 13: honor sub.enableCancelOpenOrders (default=true)
      if (sub.enableCancelOpenOrders === false) {
        results.push({ sub: sub.id, skipped: 'enableCancelOpenOrders=false' });
        continue;
      }
      const cancelRes = await cancelOpenOrders(conn.env, token, sub.accountId, payload.ticker, payload.cancelOrderType);
      results.push({ sub: sub.id, ok: true, cancel: true, cancelled: cancelRes.cancelled, filterType: payload.cancelOrderType || null });
      continue;
    }

    const incomingAction = payload.action.toLowerCase();
    const sentiment = payload.sentiment ? String(payload.sentiment).toLowerCase() : null;
    const sides = sub.sides || 'both'; // 'bullish' | 'bearish' | 'both' — per sub config
    const isolateSides = !!sub.isolateSides;
    const allowAdd = !!sub.allowAddToPosition;
    const subtractExitFromSignal = !!sub.subtractExitFromSignal;
    const useFractional = !!sub.useFractionalQuantity;

    // Step 1: Fetch current position with accurate net size
    let currentPos = null;
    try {
      const posRes = await tvApi(conn.env, '/position/list', token);
      if (posRes.ok && Array.isArray(posRes.data)) {
        currentPos = posRes.data.find(p => {
          const sym = (p.contractName || p.symbol || '').toUpperCase();
          const target = payload.ticker.toUpperCase();
          return p.accountId === sub.accountId && p.netPos !== 0 && (sym === target || sym.startsWith(target.replace(/[A-Z]\d+$/, '')));
        });
      }
    } catch {}
    const hasLong = currentPos && currentPos.netPos > 0;
    const hasShort = currentPos && currentPos.netPos < 0;
    const positionSize = currentPos ? Math.abs(currentPos.netPos) : 0;

    // Determine situation
    const isOppositeEntry = (hasLong && incomingAction === 'sell') || (hasShort && incomingAction === 'buy');
    const isSameSideEntry = (hasLong && incomingAction === 'buy') || (hasShort && incomingAction === 'sell');
    const isExitAction = incomingAction === 'exit';
    const isAddAction = incomingAction === 'add';
    const isFlatSentiment = sentiment === 'flat';
    const isScaleIn = incomingAction === 'scale_in';
    const isScaleOut = incomingAction === 'scale_out';
    
    // --- SCALE IN: add to existing position in same direction ---
    // Requires existing position; if no position, rejects (use buy/sell instead)
    if (isScaleIn) {
      if (!currentPos || currentPos.netPos === 0) {
        results.push({ sub: sub.id, ok: false, error: 'no-position-to-scale', skipped: 'scale_in requires open position' });
        continue;
      }
      // Treat as same-direction entry, skip opposite-side logic
      payload.action = currentPos.netPos > 0 ? 'buy' : 'sell';
    }
    
    // --- SCALE OUT: reduce existing position by partial qty ---
    if (isScaleOut) {
      if (!currentPos || currentPos.netPos === 0) {
        results.push({ sub: sub.id, ok: false, error: 'no-position-to-scale', skipped: 'scale_out requires open position' });
        continue;
      }
      // scaleOutPercent or scaleOutQty controls how much to close
      const scalePct = parseFloat(payload.scaleOutPercent) || 50; // default 50% close
      const currentAbs = Math.abs(currentPos.netPos);
      const qtyToClose = parseInt(payload.scaleOutQty) || Math.max(1, Math.floor(currentAbs * scalePct / 100));
      const closeAction = currentPos.netPos > 0 ? 'Sell' : 'Buy';
      
      // Place partial close order (market, no brackets)
      const partialOrder = {
        accountSpec: sub.accountSpec, accountId: sub.accountId,
        action: closeAction, symbol: payload.ticker,
        orderQty: Math.min(qtyToClose, currentAbs),
        orderType: 'Market', isAutomated: true, timeInForce: 'Day'
      };
      const partialRes = await tvApiWithAuth(kv, conn, '/order/placeorder', 'POST', partialOrder);
      results.push({
        sub: sub.id, ok: partialRes.ok, action: 'scale_out',
        qtyClosed: partialOrder.orderQty, remainingQty: currentAbs - partialOrder.orderQty,
        orderId: partialRes.data?.orderId
      });
      continue;
    }

    // --- EXIT action or sentiment=flat: exit without flipping ---
    if (isExitAction || isFlatSentiment) {
      if (!currentPos) {
        results.push({ sub: sub.id, ok: false, error: 'no-position-to-exit', skipped: 'no position open' });
        continue;
      }
      // Cancel: defaults true; if sub.defaultCancelOnExit===false or payload.cancel===false, skip
      const doCancel = payload.cancel === false ? false : (sub.defaultCancelOnExit !== false);
      if (doCancel) {
        await cancelOpenOrders(conn.env, token, sub.accountId, payload.ticker, payload.cancelOrderType);
      }

      // sentiment=flat per TradersPost spec: ALWAYS exit full position regardless of quantity
      // EXIT action: if quantity specified, partial exit; else full exit
      let actualExitQty;
      if (isFlatSentiment) {
        actualExitQty = positionSize;
      } else {
        const exitQty = payload.quantity ? parseFloat(payload.quantity) : positionSize;
        actualExitQty = exitQty;
        if (payload.quantityType === 'percent_of_position' && payload.quantity) {
          actualExitQty = Math.max(1, Math.floor(positionSize * (parseFloat(payload.quantity) / 100)));
        }
        actualExitQty = Math.min(actualExitQty, positionSize);
      }

      if (actualExitQty >= positionSize) {
        // Full exit via liquidatePosition
        const exitRes = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
          accountId: sub.accountId, symbol: payload.ticker, admin: false
        });
        results.push({ sub: sub.id, ok: exitRes.ok, status: exitRes.status, exit: true, fullExit: true, exitQty: positionSize, sentiment: sentiment, data: exitRes.data });
      } else {
        // Partial exit via opposite-direction order (Phase 13: honor sub.exitOrderType + exitTIF)
        const exitSide = hasLong ? 'Sell' : 'Buy';
        const exitOrderTypeMap = { market: 'Market', limit: 'Limit', stop_market: 'Stop', stop_limit: 'StopLimit' };
        const exitTIFMap = { day: 'Day', gtc: 'GTC', fok: 'FOK', ioc: 'IOC', 'default': 'Day' };
        const resolvedExitOT = exitOrderTypeMap[sub.exitOrderType] || 'Market';
        const resolvedExitTIF = exitTIFMap[sub.exitTIF || 'default'] || 'Day';
        const partialOrder = {
          accountSpec: sub.accountSpec, accountId: sub.accountId, action: exitSide,
          symbol: payload.ticker, orderQty: actualExitQty,
          orderType: resolvedExitOT, isAutomated: true, timeInForce: resolvedExitTIF
        };
        // Phase 13: attach price for limit/stop exit orders
        if (sub.exitOrderType === 'limit' || sub.exitOrderType === 'stop_limit') {
          partialOrder.price = payload.signalPrice || payload.limitPrice || entryPrice;
        }
        if (sub.exitOrderType === 'stop_market' || sub.exitOrderType === 'stop_limit') {
          partialOrder.stopPrice = payload.stopPrice || payload.signalPrice || entryPrice;
        }
        // Phase 13: use sub's retry config for exit order
        const exitRetries = sub.allowRetries ? (sub.maxRetries || 5) : 2;
        const exitRetryOpts = sub.allowRetries ? {
          initialDelayMs: sub.retryDelayMs || 1000,
          multiplier: sub.retryDelayMultiplier || 2,
          maxDelayMs: sub.retryMaxDelayMs || 10000
        } : null;
        const exitRes = await tvApi(conn.env, '/order/placeorder', token, 'POST', partialOrder, exitRetries, exitRetryOpts);
        results.push({ sub: sub.id, ok: exitRes.ok, status: exitRes.status, exit: true, partialExit: true, exitQty: actualExitQty, remainingQty: positionSize - actualExitQty, sentiment: sentiment, exitOrderType: resolvedExitOT, data: exitRes.data });
      }
      continue;
    }

    // --- ADD action: require existing same-side position ---
    if (isAddAction) {
      if (!currentPos) {
        results.push({ sub: sub.id, ok: false, error: 'no-position-to-add', message: 'action=add requires existing position' });
        continue;
      }
      // Add uses same side as current position
      const addSide = hasLong ? 'buy' : 'sell';
      effectivePayload.action = addSide;
      // Fall through to entry logic
    }

    // --- SIDES check: filter entries that don't match the subscription's allowed sides ---
    if (!isAddAction && !isExitAction) {
      if (sides === 'bullish' && incomingAction === 'sell' && !hasLong) {
        results.push({ sub: sub.id, ok: false, skipped: 'sides-filter', message: 'subscription sides=bullish rejects sell entry' });
        continue;
      }
      if (sides === 'bearish' && incomingAction === 'buy' && !hasShort) {
        results.push({ sub: sub.id, ok: false, skipped: 'sides-filter', message: 'subscription sides=bearish rejects buy entry' });
        continue;
      }
    }

    // --- ISOLATE SIDES: prevent closing opposite-side positions ---
    if (isolateSides && isOppositeEntry) {
      results.push({ sub: sub.id, ok: false, skipped: 'isolate-sides', message: 'isolate-sides enabled — cannot close opposite position' });
      continue;
    }

    // --- SAME-SIDE ENTRY: rejected unless allowAddToPosition is enabled ---
    if (isSameSideEntry && !allowAdd && !isAddAction) {
      results.push({ sub: sub.id, ok: false, skipped: 'same-side-entry', message: 'signal rejected: position already open on this side (enable allow-add-to-position to override)' });
      continue;
    }

    // --- EMPTY SIGNAL CHECK: no position + no entry + no cancel = reject ---
    // (This only applies if we somehow reach here with an action that doesn't do anything meaningful.)

    // --- CANCEL OPEN ORDERS: always cancel before entry if no position, opposite, or explicit cancel flag ---
    // Respect explicit cancel:false to suppress default canceling behavior
    let cancelledOrderCount = 0;
    const shouldCancel = payload.cancel === false ? false : (payload.cancel === true || !currentPos || isOppositeEntry);
    if (shouldCancel) {
      try {
        const cancelRes = await cancelOpenOrders(conn.env, token, sub.accountId, payload.ticker, payload.cancelOrderType);
        cancelledOrderCount = cancelRes.cancelled || 0;
      } catch {}
    }

    // --- EXIT OPPOSITE POSITION: wait up to 2 min for fill (per TradersPost spec) ---
    let exitedOppositeQty = 0;
    if (isOppositeEntry) {
      try {
        await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
          accountId: sub.accountId, symbol: payload.ticker, admin: false
        });
        exitedOppositeQty = positionSize;
        // Poll for exit to complete (up to 2 minutes; check every 2 seconds)
        const maxWaitMs = 120000;
        const pollIntervalMs = 2000;
        const tStart = Date.now();
        let exited = false;
        while (Date.now() - tStart < maxWaitMs) {
          await new Promise(r => setTimeout(r, pollIntervalMs));
          try {
            const posCheck = await tvApi(conn.env, '/position/list', token);
            const stillOpen = posCheck.data?.find(p => {
              const sym = (p.contractName || p.symbol || '').toUpperCase();
              return p.accountId === sub.accountId && p.netPos !== 0 && sym === payload.ticker.toUpperCase();
            });
            if (!stillOpen) { exited = true; break; }
          } catch {}
        }
        if (!exited) {
          results.push({ sub: sub.id, ok: false, error: 'exit-timeout', message: 'exit order did not fill within 2 minutes', exitedOppositeQty });
          continue;
        }
      } catch (err) {
        results.push({ sub: sub.id, ok: false, error: 'exit-failed', message: String(err) });
        continue;
      }
    }

    // --- CALCULATE ENTRY QUANTITY ---
    let entryQty;
    // If subscription has "use signal quantity" disabled, use sub's fixed quantity
    if (sub.useSignalQuantity === false) {
      entryQty = Math.max(1, Math.floor(sub.fixedQuantity || 1));
    } else {
      entryQty = await calcQuantity(effectivePayload, conn.env, token, { id: sub.accountId, name: sub.accountSpec }, sub);
    }

    // --- SUBTRACT EXIT FROM SIGNAL QUANTITY ---
    // If flipping long→short with signal qty=5 and existing long=3, enter only 2 on the new side
    if (subtractExitFromSignal && isOppositeEntry && payload.quantity) {
      const signalQty = parseFloat(payload.quantity);
      const remaining = Math.max(0, signalQty - exitedOppositeQty);
      if (remaining === 0) {
        results.push({ sub: sub.id, ok: true, exit: true, partialFlip: true, message: `flipped: closed ${exitedOppositeQty}, entered 0` });
        continue;
      }
      entryQty = remaining;
    }

    // --- APPLY QUANTITY MULTIPLIER from subscription ---
    const finalQty = useFractional
      ? Math.max(0.01, entryQty * (sub.quantityMultiplier || 1))
      : Math.max(1, Math.floor(entryQty * (sub.quantityMultiplier || 1)));

    if (finalQty <= 0) {
      results.push({ sub: sub.id, ok: false, error: 'zero-quantity', message: 'calculated quantity is zero' });
      continue;
    }

    // --- ENTER NEW POSITION ---
    const tEnterStart = Date.now();
    const { order, hasBrackets, tpPrice, slPrice } = buildOsoOrder(effectivePayload, { id: sub.accountId, name: sub.accountSpec }, finalQty, entryPrice);
    // Apply min-tick rounding per TradersPost spec:
    // round UP for sells, DOWN for buys, to nearest min_move precision
    const spec = getSpec(payload.ticker);
    if (order.price != null) {
      order.price = incomingAction === 'sell'
        ? Math.ceil(order.price / spec.tickSize) * spec.tickSize
        : Math.floor(order.price / spec.tickSize) * spec.tickSize;
    }
    if (order.stopPrice != null) {
      order.stopPrice = incomingAction === 'sell'
        ? Math.ceil(order.stopPrice / spec.tickSize) * spec.tickSize
        : Math.floor(order.stopPrice / spec.tickSize) * spec.tickSize;
    }

    const endpoint = hasBrackets ? '/order/placeOSO' : '/order/placeorder';
    // Phase 13: honor per-sub retry config (TradersPost-style) with full backoff params
    const subRetries = sub.allowRetries ? (sub.maxRetries || 5) : 2;
    const retryOpts = sub.allowRetries ? {
      initialDelayMs: sub.retryDelayMs || 1000,
      multiplier: sub.retryDelayMultiplier || 2,
      maxDelayMs: sub.retryMaxDelayMs || 10000
    } : null;
    const orderRes = await tvApi(conn.env, endpoint, token, 'POST', order, subRetries, retryOpts);
    const enterLatency = Date.now() - tEnterStart;

    // Order Queueing: if broker rejects with market-closed, queue for next market open
    let queued = false;
    if (isQueueableFailure(orderRes) && sub.enableOrderQueueing !== false) {
      const q = await queueTrade(kv, sub.id, payload.ticker, payload, strategyId, orderRes.data?.failureText || 'market-closed');
      queued = true;
      results.push({
        sub: sub.id, ok: false, queued: true, queuedUntil: q.expiresAt,
        reason: 'market-closed',
        message: 'order queued for next market open (one queued trade per sub/ticker)',
        enterLatencyMs: enterLatency
      });
      continue;
    }

    // --- SLIPPAGE CALC ---
    let slippage = null;
    if (payload.signalPrice && entryPrice) {
      slippage = {
        signalPrice: payload.signalPrice,
        estimatedFillPrice: entryPrice,
        slippagePoints: Math.abs(entryPrice - payload.signalPrice),
        slippageDollars: Math.abs(entryPrice - payload.signalPrice) * spec.pointValue * finalQty,
        adverse: (incomingAction === 'buy' && entryPrice > payload.signalPrice) || (incomingAction === 'sell' && entryPrice < payload.signalPrice)
      };
    }

    // --- TOTAL LATENCY from payload.time if provided ---
    let signalLatencyMs = null;
    if (payload.time) {
      const sigTime = new Date(payload.time).getTime();
      if (!isNaN(sigTime)) signalLatencyMs = Date.now() - sigTime;
    }

    results.push({
      sub: sub.id, ok: orderRes.ok, status: orderRes.status,
      orderId: orderRes.data?.orderId,
      brackets: hasBrackets ? { tp: tpPrice, sl: slPrice } : null,
      qty: finalQty,
      cancelledOpenOrders: cancelledOrderCount,
      exitedOpposite: isOppositeEntry ? exitedOppositeQty : 0,
      addedToPosition: isSameSideEntry && allowAdd,
      sentiment: sentiment || null,
      enterLatencyMs: enterLatency,
      signalLatencyMs,
      slippage,
      entryPrice,
      failureReason: orderRes.data?.failureReason,
      failureText: orderRes.data?.failureText
    });
    if (orderRes.ok && orderRes.data?.orderId) {
      await logTrade(kv, strategyId, sub.id, payload, orderRes.data, false, {
        entryPrice, qty: finalQty, connectionId: sub.connectionId,
        pointValue: spec?.pointValue || null,
        accountId: sub.accountId, accountSpec: sub.accountSpec
      });
    }
    
    // Fire notification to configured channels (Discord/Telegram/Email)
    if (orderRes.ok) {
      notifyChannels(kv, 'signal', {
        action: payload.action, ticker: payload.ticker, strategy: strategy.name,
        qty: finalQty, price: entryPrice || 'market'
      }).catch(() => {});
      notifyChannels(kv, 'fill', {
        action: payload.action, ticker: payload.ticker, fillPrice: entryPrice,
        qty: finalQty, account: sub.accountSpec
      }).catch(() => {});
    } else {
      notifyChannels(kv, 'error', {
        action: payload.action, ticker: payload.ticker, strategy: strategy.name,
        reason: orderRes.data?.failureText || orderRes.data?.failureReason || 'unknown'
      }).catch(() => {});
    }
  }
  const signalLog = { status: sigStatus, results, warnings: compliance.warnings, pendingApprovals: pendingApprovals.map(a => a.id) };
  const signalId = await logSignal(kv, strategyId, payload, signalLog);
  return json({
    success: true,
    id: signalId,
    logId: signalId,
    pending: hasPending,
    results,
    warnings: compliance.warnings,
    payload: {
      ticker: payload.ticker,
      action: payload.action,
      sentiment: payload.sentiment,
      signalPrice: payload.signalPrice,
      quantity: payload.quantity,
      quantityType: payload.quantityType,
      orderType: payload.orderType,
      time: payload.time,
      test: payload.test,
      takeProfit: payload.takeProfit,
      stopLoss: payload.stopLoss,
      extras: payload.extras
    }
  });
}
async function logSignal(kv, strategyId, payload, result) {
  const id = crypto.randomUUID();
  const ts = Date.now();
  await kv.put(`signal:${strategyId}:${ts}:${id}`, JSON.stringify({ id, strategyId, payload, result, timestamp: ts }), { expirationTtl: 2592000 });
  // Save idempotency marker: maps hash → signal ID, TTL 60s
  if (payload._idemDupKey) {
    await kv.put(payload._idemDupKey, id, { expirationTtl: 60 });
  }
  // Increment signalCount on strategy for accurate counter
  try {
    const strat = await kv.get(`strategy:${strategyId}`, 'json');
    if (strat) {
      strat.signalCount = (strat.signalCount || 0) + 1;
      strat.lastSignalAt = ts;
      await kv.put(`strategy:${strategyId}`, JSON.stringify(strat));
    }
  } catch {}
  return id;
}
async function logTrade(kv, strategyId, subId, payload, orderResult, paper = false, extra = {}) {
  const id = crypto.randomUUID();
  const ts = Date.now();
  // Resolve connectionId from the strategy's subscription (critical for daily-loss lookup)
  let connectionId = extra.connectionId || null;
  if (!connectionId && subId && subId !== 'paper') {
    try {
      const strat = await kv.get(`strategy:${strategyId}`, 'json');
      const sub = strat?.subscriptions?.find(s => s.id === subId);
      if (sub) connectionId = sub.connectionId;
    } catch {}
  }
  const tradeRecord = {
    id, strategyId, subscriptionId: subId, connectionId,
    ticker: payload.ticker, action: payload.action,
    quantity: payload.quantity || extra.qty || 1,
    orderId: orderResult.orderId,
    entryPrice: orderResult.fillPrice || extra.entryPrice || null,
    entryTime: ts,
    // Phase 13: tag source (manual=from UI Submit Signal, bot=from TV webhook)
    source: payload.source || 'bot',
    // Position tracking fields (populated later by trade-closure-tracker cron)
    status: paper ? 'closed' : 'open', // paper trades auto-close immediately
    exitPrice: null, exitTime: null, closedAt: null,
    pnl: paper ? (extra.pnl || 0) : null, // paper pnl can be simulated; real comes from closure
    pointValue: extra.pointValue || null,
    paper, ts, timestamp: ts // keep both for backwards compat
  };
  await kv.put(`trade:${strategyId}:${ts}:${id}`, JSON.stringify(tradeRecord), { expirationTtl: 2592000 });
  // Also index by connection for fast daily-loss lookup
  if (connectionId && !paper) {
    await kv.put(`trade-by-conn:${connectionId}:${ts}:${id}`, JSON.stringify(tradeRecord), { expirationTtl: 2592000 });
  }
  // Increment tradeCount on strategy
  try {
    const strat = await kv.get(`strategy:${strategyId}`, 'json');
    if (strat) {
      strat.tradeCount = (strat.tradeCount || 0) + 1;
      strat.lastTradeAt = ts;
      if (subId && Array.isArray(strat.subscriptions)) {
        const sub = strat.subscriptions.find(s => s.id === subId);
        if (sub) {
          sub.signalsSent = (sub.signalsSent || 0) + 1;
          if (!paper) sub.tradesFilled = (sub.tradesFilled || 0) + 1;
        }
      }
      await kv.put(`strategy:${strategyId}`, JSON.stringify(strat));
    }
  } catch {}
  return id;
}
async function handleApi(request, env, path) {
  const kv = env.PROFITBOT_KV;
  if (!kv) return json({ error: 'KV not bound' }, 500);
  const url = new URL(request.url);
  if (path === '/api/strategies' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'strategy:' });
    const strategies = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    return json({ strategies: strategies.filter(Boolean) });
  }
  const sMatch = path.match(/^\/api\/strategies\/([^/]+)$/);
  if (sMatch) {
    const id = sMatch[1];
    if (request.method === 'PUT') { await kv.put(`strategy:${id}`, JSON.stringify(await request.json())); return json({ ok: true }); }
    if (request.method === 'DELETE') { await kv.delete(`strategy:${id}`); return json({ ok: true }); }
  }
  if (path === '/api/signals' && request.method === 'GET') {
    const sid = url.searchParams.get('strategy');
    const limit = parseInt(url.searchParams.get('limit') || '100');
    const prefix = sid ? `signal:${sid}:` : 'signal:';
    const list = await kv.list({ prefix, limit });
    const signals = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    return json({ signals: signals.filter(Boolean).reverse() });
  }
  if (path === '/api/trades' && request.method === 'GET') {
    const sid = url.searchParams.get('strategy');
    const cursor = url.searchParams.get('cursor') || undefined;
    const limit = Math.min(parseInt(url.searchParams.get('limit')) || 500, 1000);
    const prefix = sid ? `trade:${sid}:` : 'trade:';
    const list = await kv.list({ prefix, limit, cursor });
    const trades = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    return json({ 
      trades: trades.filter(Boolean).reverse(), 
      cursor: list.list_complete ? null : list.cursor,
      count: trades.length,
      hasMore: !list.list_complete
    });
  }
  if (path === '/api/connections/test' && request.method === 'POST') {
    const creds = await request.json();
    const auth = await tvAuth(creds.env || 'demo', creds);
    if (!auth.accessToken) return json({ ok: false, error: auth }, 400);
    const accounts = await tvApi(creds.env, '/account/list', auth.accessToken);
    return json({ ok: true, userId: auth.userId, name: auth.name, accounts: accounts.data });
  }
  const cMatch = path.match(/^\/api\/connections\/([^/]+)$/);
  if (cMatch) {
    const id = cMatch[1];
    if (request.method === 'PUT') { await kv.put(`connection:${id}`, JSON.stringify(await request.json())); return json({ ok: true }); }
    if (request.method === 'DELETE') { await kv.delete(`connection:${id}`); await kv.delete(`token:${id}`); return json({ ok: true }); }
  }
  if (path === '/api/positions' && request.method === 'GET') {
    const cid = url.searchParams.get('connection');
    if (!cid) return json({ error: 'connection required' }, 400);
    const conn = await kv.get(`connection:${cid}`, 'json');
    if (!conn) return json({ error: 'not-found' }, 404);
    const token = await getCachedToken(kv, conn);
    if (!token) return json({ error: 'auth-failed' }, 401);
    const posRes = await tvApi(conn.env, '/position/list', token);
    const cashRes = await tvApi(conn.env, '/cashBalance/list', token);
    return json({ positions: posRes.data, cash: cashRes.data });
  }
  const exitMatch = path.match(/^\/api\/exit\/([^/]+)$/);
  if (exitMatch && request.method === 'POST') {
    const conn = await kv.get(`connection:${exitMatch[1]}`, 'json');
    if (!conn) return json({ error: 'not-found' }, 404);
    const token = await getCachedToken(kv, conn);
    if (!token) return json({ error: 'auth-failed' }, 401);
    const body = await request.json();
    const res = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
      accountId: body.accountId, symbol: body.symbol, admin: false
    });
    return json(res);
  }

  // ========== ENTERPRISE METRICS ==========
  // GET /api/metrics — aggregate execution stats across all signals and trades
  // Returns: counts by status, p50/p95/p99 latency, slippage distribution, per-strategy breakdown
  if (path === '/api/metrics' && request.method === 'GET') {
    const sinceMs = parseInt(url.searchParams.get('sinceMs') || String(7 * 86400000)); // default 7 days
    const cutoff = Date.now() - sinceMs;
    const sigList = await kv.list({ prefix: 'signal:', limit: 1000 });
    const signals = (await Promise.all(sigList.keys.map(k => kv.get(k.name, 'json')))).filter(s => s && s.timestamp >= cutoff);
    const tradeList = await kv.list({ prefix: 'trade:', limit: 1000 });
    const trades = (await Promise.all(tradeList.keys.map(k => kv.get(k.name, 'json')))).filter(t => t && t.timestamp >= cutoff);

    // Counts by status
    const statusCounts = {};
    const byStrategy = {};
    const enterLatencies = [];
    const signalLatencies = [];
    const slippages = [];
    let duplicates = 0, rejected = 0, processed = 0, blocked = 0, rateLimited = 0;

    for (const s of signals) {
      const st = s.result?.status || 'unknown';
      statusCounts[st] = (statusCounts[st] || 0) + 1;
      if (st === 'duplicate') duplicates++;
      if (st === 'rejected') rejected++;
      if (st === 'processed') processed++;
      if (st === 'blocked') blocked++;
      if (st === 'rate-limited') rateLimited++;
      byStrategy[s.strategyId] = byStrategy[s.strategyId] || { signals: 0, processed: 0, rejected: 0 };
      byStrategy[s.strategyId].signals++;
      if (st === 'processed') byStrategy[s.strategyId].processed++;
      if (st === 'rejected') byStrategy[s.strategyId].rejected++;
      if (s.result?.results) {
        for (const r of s.result.results) {
          if (typeof r.enterLatencyMs === 'number') enterLatencies.push(r.enterLatencyMs);
          if (typeof r.signalLatencyMs === 'number') signalLatencies.push(r.signalLatencyMs);
          if (r.slippage?.slippageDollars) slippages.push({
            dollars: r.slippage.slippageDollars,
            points: r.slippage.slippagePoints,
            adverse: r.slippage.adverse
          });
        }
      }
    }

    function percentile(arr, p) {
      if (!arr.length) return null;
      const sorted = [...arr].sort((a, b) => a - b);
      const idx = Math.floor(sorted.length * p / 100);
      return sorted[Math.min(idx, sorted.length - 1)];
    }

    return json({
      sinceMs,
      signalsTotal: signals.length,
      tradesTotal: trades.length,
      statusCounts,
      duplicates, rejected, processed, blocked, rateLimited,
      latency: {
        enterOrderP50: percentile(enterLatencies, 50),
        enterOrderP95: percentile(enterLatencies, 95),
        enterOrderP99: percentile(enterLatencies, 99),
        signalToWorkerP50: percentile(signalLatencies, 50),
        signalToWorkerP95: percentile(signalLatencies, 95),
        signalToWorkerP99: percentile(signalLatencies, 99),
        samplesEnter: enterLatencies.length,
        samplesSignal: signalLatencies.length
      },
      slippage: {
        samples: slippages.length,
        avgDollars: slippages.length ? slippages.reduce((s, x) => s + x.dollars, 0) / slippages.length : null,
        avgPoints: slippages.length ? slippages.reduce((s, x) => s + x.points, 0) / slippages.length : null,
        adverseCount: slippages.filter(s => s.adverse).length,
        favorableCount: slippages.filter(s => !s.adverse).length
      },
      byStrategy,
      generatedAt: Date.now()
    });
  }

  // ========== BROKER HEALTH ==========
  // GET /api/broker-health — uptime, latency, failure rate per connection
  if (path === '/api/broker-health' && request.method === 'GET') {
    const health = {};
    const connList = await kv.list({ prefix: 'connection:' });
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      // Read stored health samples (last 50 pings)
      const samplesRaw = await kv.get(`health:${conn.id}`, 'json');
      const samples = Array.isArray(samplesRaw) ? samplesRaw : [];
      const ok = samples.filter(s => s.ok).length;
      const total = samples.length;
      const latencies = samples.filter(s => s.ok && typeof s.latencyMs === 'number').map(s => s.latencyMs);
      const sortedLat = [...latencies].sort((a, b) => a - b);
      health[conn.id] = {
        name: conn.name,
        firm: conn.firm,
        env: conn.env,
        status: conn.status,
        uptimePct: total ? (ok / total) * 100 : null,
        samples: total,
        lastCheck: samples[samples.length - 1]?.ts || null,
        lastOk: samples.filter(s => s.ok).pop()?.ts || null,
        lastFailure: samples.filter(s => !s.ok).pop() || null,
        latency: {
          p50: sortedLat[Math.floor(sortedLat.length * 0.5)] || null,
          p95: sortedLat[Math.floor(sortedLat.length * 0.95)] || null,
          avg: latencies.length ? latencies.reduce((s, x) => s + x, 0) / latencies.length : null
        }
      };
    }
    return json({ connections: health, checkedAt: Date.now() });
  }

  // POST /api/broker-health/ping/{connectionId} — perform live health check, store sample
  const phMatch = path.match(/^\/api\/broker-health\/ping\/([^/]+)$/);
  if (phMatch && request.method === 'POST') {
    const conn = await kv.get(`connection:${phMatch[1]}`, 'json');
    if (!conn) return json({ error: 'not-found' }, 404);
    const t0 = Date.now();
    let ok = false, status = 0, errMsg = null;
    try {
      const token = await getCachedToken(kv, conn);
      if (!token) { errMsg = 'auth-failed'; }
      else {
        const res = await tvApi(conn.env, '/account/list', token);
        ok = res.ok; status = res.status;
        if (!res.ok) errMsg = `http-${res.status}`;
      }
    } catch (err) { errMsg = String(err); }
    const sample = { ts: Date.now(), ok, latencyMs: Date.now() - t0, status, error: errMsg };
    // Store rolling window of 50 samples
    const prev = (await kv.get(`health:${conn.id}`, 'json')) || [];
    const next = [...prev, sample].slice(-50);
    await kv.put(`health:${conn.id}`, JSON.stringify(next), { expirationTtl: 604800 });
    return json({ ok, sample });
  }

  // ========== SIGNAL REPLAY ==========
  // POST /api/signals/{signalId}/replay — re-run a past signal (optionally modified), with dry-run support
  const replayMatch = path.match(/^\/api\/signals\/([^/]+)\/replay$/);
  if (replayMatch && request.method === 'POST') {
    const signalId = replayMatch[1];
    // Signals are keyed as signal:{strategyId}:{ts}:{id}, so we have to list+find
    const list = await kv.list({ prefix: 'signal:' });
    let original = null, originalKey = null;
    for (const k of list.keys) {
      if (k.name.endsWith(':' + signalId)) {
        originalKey = k.name;
        original = await kv.get(k.name, 'json');
        break;
      }
    }
    if (!original) return json({ error: 'signal-not-found' }, 404);
    const body = await request.json();
    // Allow payload overrides and dry-run mode
    const mergedPayload = { ...original.payload, ...(body.overrides || {}) };
    if (body.dryRun) mergedPayload.test = true;
    // Tag as replay so downstream can see
    mergedPayload._replayOf = signalId;
    const strategy = await kv.get(`strategy:${original.strategyId}`, 'json');
    if (!strategy) return json({ error: 'strategy-not-found' }, 404);
    const replayReq = new Request('https://worker.local/trading/webhook/' + original.strategyId + '/' + strategy.password, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(mergedPayload)
    });
    return await handleWebhook(replayReq, env, original.strategyId, strategy.password);
  }

  // ========== ALERT RULES ==========
  // GET /api/alert-rules — list configured rules
  if (path === '/api/alert-rules' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'rule:' });
    const rules = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    return json({ rules: rules.filter(Boolean) });
  }
  // POST /api/alert-rules — create/update a rule
  if (path === '/api/alert-rules' && request.method === 'POST') {
    const rule = await request.json();
    if (!rule.id) rule.id = crypto.randomUUID();
    rule.updatedAt = Date.now();
    if (!rule.createdAt) rule.createdAt = Date.now();
    await kv.put(`rule:${rule.id}`, JSON.stringify(rule));
    return json({ ok: true, rule });
  }
  // DELETE /api/alert-rules/{id}
  const rMatch = path.match(/^\/api\/alert-rules\/([^/]+)$/);
  if (rMatch && request.method === 'DELETE') {
    await kv.delete(`rule:${rMatch[1]}`);
    return json({ ok: true });
  }

  // ========== QUEUED TRADES ==========
  // GET /api/queued-trades — list all queued orders waiting for next market open
  if (path === '/api/queued-trades' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'queued:' });
    const queued = (await Promise.all(list.keys.map(k => kv.get(k.name, 'json')))).filter(Boolean);
    return json({ queued: queued.sort((a,b) => b.queuedAt - a.queuedAt) });
  }
  // DELETE /api/queued-trades/{subId}/{ticker} — remove a queued trade
  const qtMatch = path.match(/^\/api\/queued-trades\/([^/]+)\/([^/]+)$/);
  if (qtMatch && request.method === 'DELETE') {
    await kv.delete(`queued:${qtMatch[1]}:${qtMatch[2]}`);
    return json({ ok: true });
  }
  // POST /api/queued-trades/{subId}/{ticker}/retry — manually retry a queued trade
  const qrMatch = path.match(/^\/api\/queued-trades\/([^/]+)\/([^/]+)\/retry$/);
  if (qrMatch && request.method === 'POST') {
    const entry = await kv.get(`queued:${qrMatch[1]}:${qrMatch[2]}`, 'json');
    if (!entry) return json({ error: 'not-found' }, 404);
    const strategy = await kv.get(`strategy:${entry.strategyId}`, 'json');
    if (!strategy) return json({ error: 'strategy-not-found' }, 404);
    // Re-submit via webhook pipeline
    const replayReq = new Request('https://worker.local/trading/webhook/' + entry.strategyId + '/' + strategy.password, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...entry.payload, _approved: true }) // skip idempotency check
    });
    const result = await handleWebhook(replayReq, env, entry.strategyId, strategy.password);
    // Remove from queue on success
    const resData = await result.clone().json().catch(() => null);
    if (resData?.success) await kv.delete(`queued:${qrMatch[1]}:${qrMatch[2]}`);
    return result;
  }

  // GET /api/approvals — list pending approvals
  if (path === '/api/approvals' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'approval:' });
    const approvals = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    const now = Date.now();
    const pending = approvals.filter(a => a && a.status === 'pending' && (!a.expiresAt || a.expiresAt > now));
    return json({ approvals: pending.sort((a,b) => b.timestamp - a.timestamp) });
  }

  // POST /api/approvals/{id}/decide — approve or reject a pending signal
  const aMatch = path.match(/^\/api\/approvals\/([^/]+)\/decide$/);
  if (aMatch && request.method === 'POST') {
    const approvalId = aMatch[1];
    const approval = await kv.get(`approval:${approvalId}`, 'json');
    if (!approval) return json({ error: 'approval-not-found' }, 404);
    if (approval.status !== 'pending') return json({ error: 'already-decided', status: approval.status }, 400);
    const body = await request.json();
    const decision = body.approved ? 'approved' : 'rejected';
    approval.status = decision;
    approval.decidedAt = Date.now();
    await kv.put(`approval:${approvalId}`, JSON.stringify(approval), { expirationTtl: 86400 });
    if (body.approved) {
      // Re-enter the webhook pipeline with _approved flag, narrowed to the one subscription
      const strategy = await kv.get(`strategy:${approval.strategyId}`, 'json');
      if (strategy) {
        // Temporarily narrow subscriptions to just the approved one, mark payload._approved
        const singleSubStrat = { ...strategy, subscriptions: strategy.subscriptions.filter(s => s.id === approval.subscriptionId) };
        const innerReq = new Request('https://worker.local/trading/webhook/' + approval.strategyId + '/' + strategy.password, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...approval.payload, _approved: true, _approvalId: approvalId })
        });
        // Save narrowed strategy temporarily... instead, just call subroutine
        // Easier path: mark _approved on the original payload and call handleWebhook via re-fetch
        const tempKey = `strategy:${approval.strategyId}`;
        await kv.put(tempKey, JSON.stringify(singleSubStrat));
        const res = await handleWebhook(innerReq, env, approval.strategyId, strategy.password);
        // Restore original strategy
        await kv.put(tempKey, JSON.stringify(strategy));
        return res;
      }
    }
    return json({ ok: true, status: decision });
  }
  
  // POST /api/kill — liquidate all positions on all connections
  if (path === '/api/kill' && request.method === 'POST') {
    const connList = await kv.list({ prefix: 'connection:' });
    const results = [];
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      const token = await getCachedToken(kv, conn);
      if (!token) { results.push({ conn: conn.id, error: 'auth' }); continue; }
      const posRes = await tvApi(conn.env, '/position/list', token);
      const positions = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
      for (const p of positions) {
        const r = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
          accountId: p.accountId, symbol: p.contractName || p.symbol, admin: false
        });
        results.push({ conn: conn.id, symbol: p.contractName || p.symbol, ok: r.ok });
      }
    }
    return json({ ok: true, results });
  }
  
  // POST /api/autoflat — close all open positions (called by client timer OR cron at 16:55 ET)
  // This is the CRITICAL Apex compliance feature: all positions MUST be flat by 16:59 ET
  // We trigger at 16:55 ET to leave a 4-minute safety buffer
  // RETRIES: If liquidation fails, we retry up to 3 times with verification
  if (path === '/api/autoflat' && request.method === 'POST') {
    const body = await request.json().catch(() => ({}));
    const reason = body.reason || 'manual';
    const auditKey = 'autoflat:' + Date.now();
    const connList = await kv.list({ prefix: 'connection:' });
    const results = [];
    let totalClosed = 0, totalFailed = 0, totalVerified = 0;
    
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn || conn.paused) continue;
      const token = await getCachedToken(kv, conn);
      if (!token) { results.push({ conn: conn.id, error: 'auth-failed' }); totalFailed++; continue; }
      
      // Get initial state
      let posRes = await tvApi(conn.env, '/position/list', token);
      let positions = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
      if (positions.length === 0) {
        results.push({ conn: conn.id, ok: true, nothing: true });
        continue;
      }
      
      // Cancel ALL pending orders on each account first (CRITICAL - prevents new fills during flat)
      const accountIds = [...new Set(positions.map(p => p.accountId))];
      for (const accId of accountIds) {
        const ordersRes = await tvApi(conn.env, '/order/list', token);
        const pending = (ordersRes.data || []).filter(o => o.accountId === accId && ['Working', 'Pending', 'Acknowledged'].includes(o.ordStatus));
        for (const ord of pending) {
          try {
            await tvApi(conn.env, '/order/cancelorder', token, 'POST', { orderId: ord.id });
          } catch (e) { /* best effort */ }
        }
      }
      
      // Liquidate each position with retry logic
      for (const p of positions) {
        let attempt = 0, success = false, lastErr = null;
        while (attempt < 3 && !success) {
          attempt++;
          const r = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
            accountId: p.accountId, symbol: p.contractName || p.symbol, admin: false
          });
          if (r.ok) {
            success = true;
            totalClosed++;
          } else {
            lastErr = r.error || r.status;
            // Wait before retry: 300ms, 800ms, 2000ms
            if (attempt < 3) await new Promise(res => setTimeout(res, attempt === 1 ? 300 : 800));
          }
        }
        results.push({ conn: conn.id, accountId: p.accountId, symbol: p.contractName || p.symbol, qty: p.netPos, ok: success, attempts: attempt, err: lastErr });
        if (!success) totalFailed++;
      }
      
      // VERIFICATION: Re-fetch positions to confirm they are actually flat
      await new Promise(res => setTimeout(res, 1500)); // Wait for fills to settle
      posRes = await tvApi(conn.env, '/position/list', token);
      const stillOpen = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
      if (stillOpen.length === 0) totalVerified++;
      else {
        // CRITICAL: positions still open after liquidation attempts — alert
        results.push({ conn: conn.id, ALERT: 'POSITIONS_STILL_OPEN', count: stillOpen.length, positions: stillOpen.map(p => ({ symbol: p.contractName || p.symbol, qty: p.netPos })) });
      }
    }
    
    // Log to audit with full details
    await kv.put(auditKey, JSON.stringify({ ts: Date.now(), reason, totalClosed, totalFailed, totalVerified, results }), { expirationTtl: 86400 * 30 });
    
    // If any position STILL open after all retries, flag as critical alert
    const criticalFailures = results.filter(r => r.ALERT === 'POSITIONS_STILL_OPEN');
    if (criticalFailures.length > 0) {
      await kv.put('alert:autoflat-failed:' + Date.now(), JSON.stringify({ ts: Date.now(), reason, criticalFailures }), { expirationTtl: 86400 * 7 });
    }
    
    return json({ ok: totalFailed === 0 && criticalFailures.length === 0, reason, totalClosed, totalFailed, totalVerified, criticalFailures: criticalFailures.length, results });
  }
  
  // GET /api/autoflat/history — recent auto-flat operations
  if (path === '/api/autoflat/history' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'autoflat:' });
    const items = [];
    for (const k of list.keys.slice(0, 20)) {
      const v = await kv.get(k.name, 'json');
      if (v) items.push({ key: k.name, ...v });
    }
    return json({ items: items.sort((a, b) => b.ts - a.ts) });
  }
  
  // GET /api/contract-expiry — list of known futures contracts with ACCURATE expiry dates per product
  if (path === '/api/contract-expiry' && request.method === 'GET') {
    const now = new Date();
    const currentYear = now.getFullYear();
    const contracts = [
      { root: 'ES', name: 'E-mini S&P 500', monthCodes: ['H','M','U','Z'], tickSize: 0.25, tickValue: 12.50, group: 'equity' },
      { root: 'NQ', name: 'E-mini Nasdaq 100', monthCodes: ['H','M','U','Z'], tickSize: 0.25, tickValue: 5.00, group: 'equity' },
      { root: 'MNQ', name: 'Micro E-mini Nasdaq 100', monthCodes: ['H','M','U','Z'], tickSize: 0.25, tickValue: 0.50, group: 'equity' },
      { root: 'MES', name: 'Micro E-mini S&P 500', monthCodes: ['H','M','U','Z'], tickSize: 0.25, tickValue: 1.25, group: 'equity' },
      { root: 'RTY', name: 'E-mini Russell 2000', monthCodes: ['H','M','U','Z'], tickSize: 0.10, tickValue: 5.00, group: 'equity' },
      { root: 'M2K', name: 'Micro E-mini Russell 2000', monthCodes: ['H','M','U','Z'], tickSize: 0.10, tickValue: 0.50, group: 'equity' },
      { root: 'YM', name: 'E-mini Dow', monthCodes: ['H','M','U','Z'], tickSize: 1.00, tickValue: 5.00, group: 'equity' },
      { root: 'MYM', name: 'Micro E-mini Dow', monthCodes: ['H','M','U','Z'], tickSize: 1.00, tickValue: 0.50, group: 'equity' },
      { root: 'CL', name: 'Crude Oil', monthCodes: 'FGHJKMNQUVXZ'.split(''), tickSize: 0.01, tickValue: 10.00, group: 'energy' },
      { root: 'MCL', name: 'Micro Crude Oil', monthCodes: 'FGHJKMNQUVXZ'.split(''), tickSize: 0.01, tickValue: 1.00, group: 'energy' },
      { root: 'NG', name: 'Natural Gas', monthCodes: 'FGHJKMNQUVXZ'.split(''), tickSize: 0.001, tickValue: 10.00, group: 'energy' },
      { root: 'GC', name: 'Gold', monthCodes: ['G','J','M','Q','V','Z'], tickSize: 0.10, tickValue: 10.00, group: 'metals' },
      { root: 'MGC', name: 'Micro Gold', monthCodes: ['G','J','M','Q','V','Z'], tickSize: 0.10, tickValue: 1.00, group: 'metals' },
      { root: 'SI', name: 'Silver', monthCodes: ['H','K','N','U','Z'], tickSize: 0.005, tickValue: 25.00, group: 'metals' }
    ];
    function thirdFriday(year, monthIdx) {
      let fridays = 0;
      for (let d = 1; d <= 21; d++) {
        const dt = new Date(year, monthIdx, d);
        if (dt.getDay() === 5 && ++fridays === 3) return dt;
      }
      return new Date(year, monthIdx, 15);
    }
    function secondThursday(year, monthIdx) {
      let thursdays = 0;
      for (let d = 1; d <= 14; d++) {
        const dt = new Date(year, monthIdx, d);
        if (dt.getDay() === 4 && ++thursdays === 2) return dt;
      }
      return new Date(year, monthIdx, 8);
    }
    function businessDaysBefore(date, n) {
      const d = new Date(date); let count = 0;
      while (count < n) { d.setDate(d.getDate() - 1); const dow = d.getDay(); if (dow !== 0 && dow !== 6) count++; }
      return d;
    }
    function lastBusinessDay(year, monthIdx) {
      const d = new Date(year, monthIdx + 1, 0);
      while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() - 1);
      return d;
    }
    function computeDates(contract, monthCode, year) {
      const monthIdx = 'FGHJKMNQUVXZ'.indexOf(monthCode);
      if (contract.group === 'equity') {
        return { expiry: thirdFriday(year, monthIdx), rollover: secondThursday(year, monthIdx) };
      } else if (contract.group === 'energy') {
        const priorMonth = monthIdx - 1;
        const base = priorMonth < 0 ? new Date(year - 1, 11, 25) : new Date(year, priorMonth, 25);
        const expiry = businessDaysBefore(base, 3);
        return { expiry, rollover: new Date(expiry.getTime() - 10 * 86400000) };
      } else {
        const priorMonth = monthIdx - 1;
        const base = priorMonth < 0 ? lastBusinessDay(year - 1, 11) : lastBusinessDay(year, priorMonth);
        const expiry = businessDaysBefore(base, 2);
        return { expiry, rollover: new Date(expiry.getTime() - 14 * 86400000) };
      }
    }
    const result = contracts.map(c => {
      const candidates = [];
      for (const mc of c.monthCodes) {
        for (let y = currentYear; y <= currentYear + 1; y++) {
          const { expiry, rollover } = computeDates(c, mc, y);
          if (rollover > now) candidates.push({ monthCode: mc, year: y, expiry, rollover });
        }
      }
      candidates.sort((a, b) => a.rollover - b.rollover);
      const active = candidates[0];
      const next = candidates[1];
      const daysToRollover = active ? Math.floor((active.rollover - now) / 86400000) : null;
      const daysToExpiry = active ? Math.floor((active.expiry - now) / 86400000) : null;
      const symbol = active ? c.root + active.monthCode + String(active.year).slice(-1) : c.root;
      const nextSymbol = next ? c.root + next.monthCode + String(next.year).slice(-1) : null;
      return { root: c.root, name: c.name, symbol, nextSymbol, rollover: active?.rollover, expiry: active?.expiry, daysToRollover, daysToExpiry, tickSize: c.tickSize, tickValue: c.tickValue, group: c.group };
    });
    return json({ contracts: result, now: now.toISOString() });
  }
  
  // GET /api/daily-loss/:connId — check daily loss status
  if (path.startsWith('/api/daily-loss/') && !path.endsWith('/unlock') && request.method === 'GET') {
    const connId = path.split('/')[3];
    const conn = await kv.get('connection:' + connId, 'json');
    if (!conn) return json({ error: 'connection-not-found' }, 404);
    const today = new Date().toISOString().slice(0, 10);
    const lockKey = 'daily-lock:' + connId + ':' + today;
    const lock = await kv.get(lockKey, 'json');
    const tradesList = await kv.list({ prefix: 'trade:' });
    let dailyPnl = 0, dailyCount = 0;
    const todayStart = new Date(today + 'T00:00:00Z').getTime();
    for (const k of tradesList.keys) {
      const t = await kv.get(k.name, 'json');
      if (!t || t.connectionId !== connId) continue;
      if ((t.ts || t.closedAt || 0) < todayStart) continue;
      if (t.pnl) { dailyPnl += t.pnl; dailyCount++; }
    }
    return json({ connId, dailyPnl, dailyCount, dailyLossLimit: conn.dailyLossLimit || 0, locked: !!lock, lockedAt: lock?.ts, lockedReason: lock?.reason });
  }
  
  // POST /api/daily-loss/:connId/unlock
  if (path.match(/^\/api\/daily-loss\/[^/]+\/unlock$/) && request.method === 'POST') {
    const connId = path.split('/')[3];
    const today = new Date().toISOString().slice(0, 10);
    await kv.delete('daily-lock:' + connId + ':' + today);
    return json({ ok: true, unlocked: true, connId });
  }
  
  // GET /api/broker-health/all — ping all connected brokers with latency
  if (path === '/api/broker-health/all' && request.method === 'GET') {
    const connList = await kv.list({ prefix: 'connection:' });
    const results = [];
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      const startMs = Date.now();
      let status = 'unknown', latency = null, error = null, accountsCount = 0;
      try {
        const token = await getCachedToken(kv, conn);
        if (!token) { status = 'auth-failed'; error = 'Authentication failed'; }
        else {
          const accRes = await tvApi(conn.env, '/account/list', token);
          latency = Date.now() - startMs;
          if (accRes.ok) { status = latency < 1000 ? 'healthy' : latency < 3000 ? 'slow' : 'degraded'; accountsCount = (accRes.data || []).length; }
          else { status = 'api-error'; error = accRes.error || 'API error'; }
        }
      } catch (e) { status = 'network-error'; error = String(e.message || e); }
      results.push({ connId: conn.id, connName: conn.name, firm: conn.firm, env: conn.env, status, latency, error, accountsCount, lastCheck: Date.now() });
      await kv.put('health:' + conn.id, JSON.stringify({ status, latency, error, ts: Date.now() }), { expirationTtl: 600 });
    }
    return json({ connections: results, ts: Date.now() });
  }
  
  // POST /api/reconcile — compare local state vs Tradovate actual positions
  if (path === '/api/reconcile' && request.method === 'POST') {
    const connList = await kv.list({ prefix: 'connection:' });
    const discrepancies = [];
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      const token = await getCachedToken(kv, conn);
      if (!token) { discrepancies.push({ connId: conn.id, error: 'auth-failed' }); continue; }
      const posRes = await tvApi(conn.env, '/position/list', token);
      const tvPositions = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
      const cachedKey = 'positions-cache:' + conn.id;
      const cached = (await kv.get(cachedKey, 'json')) || [];
      for (const p of tvPositions) {
        const symbol = p.contractName || p.symbol;
        const local = cached.find(x => x.accountId === p.accountId && x.symbol === symbol);
        if (!local) discrepancies.push({ connId: conn.id, type: 'unknown-position', accountId: p.accountId, symbol, qty: p.netPos });
        else if (local.qty !== p.netPos) discrepancies.push({ connId: conn.id, type: 'qty-mismatch', accountId: p.accountId, symbol, localQty: local.qty, actualQty: p.netPos });
      }
      for (const x of cached) {
        const found = tvPositions.find(p => p.accountId === x.accountId && (p.contractName || p.symbol) === x.symbol);
        if (!found) discrepancies.push({ connId: conn.id, type: 'phantom-position', accountId: x.accountId, symbol: x.symbol, localQty: x.qty });
      }
      await kv.put(cachedKey, JSON.stringify(tvPositions.map(p => ({ accountId: p.accountId, symbol: p.contractName || p.symbol, qty: p.netPos }))), { expirationTtl: 3600 });
    }
    return json({ ok: discrepancies.length === 0, discrepancies, ts: Date.now() });
  }
  
  // GET /api/alerts — recent critical alerts
  if (path === '/api/alerts' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'alert:' });
    const items = [];
    for (const k of list.keys.slice(0, 50)) {
      const v = await kv.get(k.name, 'json');
      if (v) items.push({ key: k.name, ...v });
    }
    return json({ items: items.sort((a, b) => b.ts - a.ts) });
  }
  
  // GET /api/ping — worker health check with uptime and recent activity
  if (path === '/api/ping' && request.method === 'GET') {
    const startKey = 'worker:start';
    let startTs = await kv.get(startKey);
    if (!startTs) { startTs = String(Date.now()); await kv.put(startKey, startTs, { expirationTtl: 86400 * 7 }); }
    const uptime = Date.now() - parseInt(startTs);
    const etNow = wGetETNow();
    // Update last-ping timestamp for dead-man-switch
    await kv.put('deadman:lastping', String(Date.now()), { expirationTtl: 3600 });
    return json({ 
      ok: true, 
      version: '2.2', 
      time: new Date().toISOString(), 
      etTime: etNow.hour + ':' + String(etNow.minute).padStart(2, '0'),
      uptime,
      nextAutoFlat: etNow.totalMin < 16 * 60 + 55 ? '16:55 ET today' : '16:55 ET tomorrow'
    });
  }
  
  // POST /api/deadman/arm — arm the dead man switch (client pings every 30s)
  // If no ping received within configured timeout, auto-flat triggers
  if (path === '/api/deadman/arm' && request.method === 'POST') {
    const body = await request.json().catch(() => ({}));
    const timeoutSec = Math.min(Math.max(body.timeoutSec || 120, 60), 600); // 1-10 min
    await kv.put('deadman:config', JSON.stringify({ enabled: true, timeoutSec, armedAt: Date.now() }), { expirationTtl: 86400 });
    await kv.put('deadman:lastping', String(Date.now()), { expirationTtl: 3600 });
    return json({ ok: true, armed: true, timeoutSec });
  }
  
  // POST /api/deadman/disarm — disarm the dead man switch
  if (path === '/api/deadman/disarm' && request.method === 'POST') {
    await kv.delete('deadman:config');
    return json({ ok: true, disarmed: true });
  }
  
  // POST /api/deadman/heartbeat — client sends this every 30s when deadman is armed
  if (path === '/api/deadman/heartbeat' && request.method === 'POST') {
    await kv.put('deadman:lastping', String(Date.now()), { expirationTtl: 3600 });
    return json({ ok: true, ts: Date.now() });
  }
  
  // GET /api/deadman/status
  if (path === '/api/deadman/status' && request.method === 'GET') {
    const config = await kv.get('deadman:config', 'json');
    const lastPing = await kv.get('deadman:lastping');
    if (!config || !config.enabled) return json({ enabled: false });
    const staleness = lastPing ? Date.now() - parseInt(lastPing) : Infinity;
    return json({ enabled: true, timeoutSec: config.timeoutSec, lastPing: lastPing ? parseInt(lastPing) : null, stalenessSec: Math.floor(staleness / 1000), overdue: staleness > config.timeoutSec * 1000 });
  }
  
  // GET /api/exchange-calendar — CME half-days and holidays
  if (path === '/api/exchange-calendar' && request.method === 'GET') {
    // CME Group Equity Index holidays & early closes (updated for 2026)
    // Full close: exchange is closed all day
    // Early close: CME closes 13:00 ET (1:00 PM ET) instead of 17:00
    const calendar = [
      // 2026 holidays
      { date: '2026-01-01', type: 'closed', name: 'New Year\'s Day' },
      { date: '2026-01-19', type: 'closed', name: 'Martin Luther King Jr. Day' },
      { date: '2026-02-16', type: 'closed', name: 'Presidents Day' },
      { date: '2026-04-03', type: 'closed', name: 'Good Friday' },
      { date: '2026-05-25', type: 'closed', name: 'Memorial Day' },
      { date: '2026-06-19', type: 'closed', name: 'Juneteenth' },
      { date: '2026-07-03', type: 'early', name: 'Independence Day (observed)', closeET: '13:00' },
      { date: '2026-07-04', type: 'closed', name: 'Independence Day' },
      { date: '2026-09-07', type: 'closed', name: 'Labor Day' },
      { date: '2026-11-26', type: 'closed', name: 'Thanksgiving' },
      { date: '2026-11-27', type: 'early', name: 'Day after Thanksgiving', closeET: '13:00' },
      { date: '2026-12-24', type: 'early', name: 'Christmas Eve', closeET: '13:00' },
      { date: '2026-12-25', type: 'closed', name: 'Christmas' },
      { date: '2026-12-31', type: 'early', name: 'New Year\'s Eve', closeET: '13:00' },
      // 2027
      { date: '2027-01-01', type: 'closed', name: 'New Year\'s Day' },
      { date: '2027-01-18', type: 'closed', name: 'Martin Luther King Jr. Day' }
    ];
    const today = new Date().toISOString().slice(0, 10);
    const nowET = wGetETNow();
    const todayEntry = calendar.find(c => c.date === today);
    const nextHoliday = calendar.find(c => c.date >= today);
    return json({ 
      calendar, 
      today: todayEntry ? { ...todayEntry, isToday: true } : null,
      nextHoliday,
      autoFlatToday: todayEntry?.type === 'early' ? '12:55 ET' : '16:55 ET'
    });
  }
  
  // POST /api/simulate — dry-run a signal to see what WOULD happen (no orders placed)
  // This is identical to ?test=true but more detailed — returns validation, compliance, sizing
  if (path === '/api/simulate' && request.method === 'POST') {
    const body = await request.json().catch(() => ({}));
    const { strategyId, payload } = body;
    if (!strategyId || !payload) return json({ error: 'strategyId and payload required' }, 400);
    const strategy = await kv.get('strategy:' + strategyId, 'json');
    if (!strategy) return json({ error: 'strategy-not-found' }, 404);
    
    const results = { ok: true, checks: [], warnings: [], blocks: [], preview: null };
    
    // Validation
    const errors = validatePayload(payload);
    if (errors.length) {
      results.ok = false;
      results.blocks.push(...errors.map(e => `Validation: ${e.message}`));
    } else {
      results.checks.push('✓ Payload validation passed');
    }
    
    // Compliance
    const compliance = apexCheck(payload, strategy);
    if (compliance.blocks.length) {
      results.ok = false;
      results.blocks.push(...compliance.blocks.map(b => `Apex: ${b}`));
    } else {
      results.checks.push('✓ Apex compliance passed');
    }
    if (compliance.warnings.length) results.warnings.push(...compliance.warnings);
    
    // Daily loss check
    const actionLower = String(payload.action || '').toLowerCase();
    const isNewEntry = ['buy', 'sell'].includes(actionLower);
    if (isNewEntry && strategy.subscriptions?.length) {
      const today = new Date().toISOString().slice(0, 10);
      for (const sub of strategy.subscriptions) {
        const lock = await kv.get('daily-lock:' + sub.connectionId + ':' + today, 'json');
        if (lock) {
          results.ok = false;
          results.blocks.push(`Daily loss lock: ${sub.connectionId} locked — ${lock.reason}`);
        }
      }
      if (!results.blocks.length) results.checks.push('✓ No daily-loss lockdowns');
    }
    
    // Build preview OSO for first subscription
    if (strategy.subscriptions?.length && isNewEntry) {
      const sub = strategy.subscriptions[0];
      const conn = await kv.get('connection:' + sub.connectionId, 'json');
      const entryPrice = payload.signalPrice || payload.limitPrice || 0;
      try {
        const { order, hasBrackets, tpPrice, slPrice } = buildOsoOrder(payload, { id: sub.accountId, name: sub.accountSpec }, payload.quantity || 1, entryPrice);
        results.preview = { broker: conn?.firm, env: conn?.env, accountSpec: sub.accountSpec, hasBrackets, entry: entryPrice, tp: tpPrice, sl: slPrice, endpoint: hasBrackets ? '/order/placeOSO' : '/order/placeorder', order };
        results.checks.push(hasBrackets ? '✓ OSO bracket order built (TP+SL attached)' : '⚠ No brackets — entry only');
        if (conn?.firm === 'apex' && !hasBrackets) {
          results.ok = false;
          results.blocks.push('Apex Hard-Stop: order without TP+SL bracket will be rejected by Tradovate');
        }
      } catch (e) {
        results.warnings.push('Could not build preview: ' + e.message);
      }
    }
    
    // Session check
    const et = wGetETNow();
    if (et.day >= 1 && et.day <= 5 && et.totalMin >= 16 * 60 + 55 && et.totalMin <= 17 * 60 + 15) {
      results.warnings.push('Past 16:55 ET — Apex will reject entries');
    }
    
    return json(results);
  }
  
  // POST /api/notifications/channels — save Discord / Telegram / Email config
  if (path === '/api/notifications/channels' && request.method === 'POST') {
    const body = await request.json();
    await kv.put('notif:channels', JSON.stringify(body), { expirationTtl: 86400 * 365 });
    return json({ ok: true, saved: Object.keys(body) });
  }
  
  // GET /api/notifications/channels — read config
  if (path === '/api/notifications/channels' && request.method === 'GET') {
    const cfg = await kv.get('notif:channels', 'json') || {};
    // Never send back secrets — mask them
    const masked = { ...cfg };
    if (masked.discordWebhookUrl) masked.discordWebhookUrl = masked.discordWebhookUrl.replace(/\/[^/]+$/, '/****');
    if (masked.telegramBotToken) masked.telegramBotToken = masked.telegramBotToken.slice(0, 8) + '****';
    return json(masked);
  }
  
  // POST /api/notifications/test — send test notification to all configured channels
  if (path === '/api/notifications/test' && request.method === 'POST') {
    const cfg = await kv.get('notif:channels', 'json') || {};
    const results = [];
    const testMsg = { title: '🧪 ProfitBot Terminal - Test', body: 'Notification test at ' + new Date().toISOString(), level: 'info' };
    if (cfg.discordEnabled && cfg.discordWebhookUrl) {
      try {
        const r = await fetch(cfg.discordWebhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content: testMsg.title + '\n' + testMsg.body }) });
        results.push({ channel: 'discord', ok: r.ok, status: r.status });
      } catch (e) { results.push({ channel: 'discord', ok: false, error: String(e) }); }
    }
    if (cfg.telegramEnabled && cfg.telegramBotToken && cfg.telegramChatId) {
      try {
        const url = `https://api.telegram.org/bot${cfg.telegramBotToken}/sendMessage`;
        const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: cfg.telegramChatId, text: testMsg.title + '\n' + testMsg.body, parse_mode: 'Markdown' }) });
        results.push({ channel: 'telegram', ok: r.ok, status: r.status });
      } catch (e) { results.push({ channel: 'telegram', ok: false, error: String(e) }); }
    }
    if (cfg.emailEnabled && cfg.emailResendKey && cfg.emailTo) {
      try {
        const r = await fetch('https://api.resend.com/emails', { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg.emailResendKey },
          body: JSON.stringify({ from: cfg.emailFrom || 'onboarding@resend.dev', to: cfg.emailTo, subject: testMsg.title, text: testMsg.body })
        });
        results.push({ channel: 'email', ok: r.ok, status: r.status });
      } catch (e) { results.push({ channel: 'email', ok: false, error: String(e) }); }
    }
    return json({ ok: results.every(r => r.ok), results });
  }
  
  // POST /api/broker-failover/config — save failover config
  if (path === '/api/broker-failover/config' && request.method === 'POST') {
    const body = await request.json();
    await kv.put('failover:config', JSON.stringify(body), { expirationTtl: 86400 * 365 });
    return json({ ok: true });
  }
  // GET /api/broker-failover/config
  if (path === '/api/broker-failover/config' && request.method === 'GET') {
    return json(await kv.get('failover:config', 'json') || {});
  }
  
  // ========== ADVANCED: RISK EXPOSURE ANALYSIS ==========
  // GET /api/risk/exposure — current portfolio risk across all connections
  if (path === '/api/risk/exposure' && request.method === 'GET') {
    const connList = await kv.list({ prefix: 'connection:' });
    const exposure = [];
    let totalPositions = 0, totalNotional = 0, totalUnrealized = 0;
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      const res = await tvApiWithAuth(kv, conn, '/position/list');
      if (!res.ok) continue;
      const positions = (res.data || []).filter(p => p.netPos && p.netPos !== 0);
      for (const p of positions) {
        const pointValue = getSpec(p.contractName || p.symbol)?.pointValue || 1;
        const notional = Math.abs(p.netPos) * (p.netPrice || 0) * pointValue;
        const unrealized = (p.netPrice - (p.avgPrice || p.netPrice)) * p.netPos * pointValue;
        exposure.push({
          connId: conn.id, connName: conn.name,
          accountId: p.accountId, symbol: p.contractName || p.symbol,
          qty: p.netPos, direction: p.netPos > 0 ? 'long' : 'short',
          avgPrice: p.avgPrice, markPrice: p.netPrice,
          notional, unrealized
        });
        totalPositions++;
        totalNotional += notional;
        totalUnrealized += unrealized;
      }
    }
    // Correlation warning: multiple positions in same symbol or same direction on correlated symbols
    const symbolConcentration = {};
    for (const e of exposure) {
      symbolConcentration[e.symbol] = (symbolConcentration[e.symbol] || 0) + e.notional;
    }
    const warnings = [];
    for (const [sym, val] of Object.entries(symbolConcentration)) {
      const pct = (val / totalNotional) * 100;
      if (pct > 50) warnings.push(`High concentration: ${sym} = ${pct.toFixed(0)}% of portfolio`);
    }
    return json({ positions: exposure, totalPositions, totalNotional, totalUnrealized, warnings });
  }
  
  // ========== ADVANCED: WEBHOOK LATENCY MONITOR ==========
  // GET /api/latency/stats — signal-to-fill latency statistics
  if (path === '/api/latency/stats' && request.method === 'GET') {
    const sigList = await kv.list({ prefix: 'signal:', limit: 500 });
    const latencies = [];
    for (const k of sigList.keys) {
      const s = await kv.get(k.name, 'json');
      if (!s?.results) continue;
      for (const r of s.results) {
        if (r.enterLatencyMs) latencies.push({ latency: r.enterLatencyMs, ts: s.timestamp });
      }
    }
    if (latencies.length === 0) return json({ empty: true });
    const sorted = latencies.map(l => l.latency).sort((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length * 0.5)];
    const p95 = sorted[Math.floor(sorted.length * 0.95)];
    const p99 = sorted[Math.floor(sorted.length * 0.99)];
    const mean = sorted.reduce((s, v) => s + v, 0) / sorted.length;
    const max = sorted[sorted.length - 1];
    const min = sorted[0];
    return json({ count: sorted.length, min, max, mean, p50, p95, p99, recent: latencies.slice(-20).reverse() });
  }
  
  // ========== ADVANCED: BACKTEST ENGINE (simple) ==========
  // POST /api/backtest — run a historical simulation against provided trade data
  if (path === '/api/backtest' && request.method === 'POST') {
    const body = await request.json();
    const { signals, startingEquity = 50000, riskPercent = 1 } = body;
    if (!Array.isArray(signals)) return json({ error: 'signals array required' }, 400);
    
    let equity = startingEquity, peak = startingEquity, maxDD = 0;
    let wins = 0, losses = 0, grossProfit = 0, grossLoss = 0;
    const curve = [{ ts: 0, equity }];
    let openPosition = null;
    
    for (const sig of signals) {
      const { action, price, timestamp, pnl: overridePnl } = sig;
      if (action === 'buy' || action === 'sell') {
        if (openPosition) {
          // Close existing first
          const exitPrice = price;
          const pnl = openPosition.direction === 'long' 
            ? (exitPrice - openPosition.entry) * openPosition.qty
            : (openPosition.entry - exitPrice) * openPosition.qty;
          equity += pnl;
          if (pnl > 0) { wins++; grossProfit += pnl; }
          else if (pnl < 0) { losses++; grossLoss += Math.abs(pnl); }
          peak = Math.max(peak, equity);
          maxDD = Math.max(maxDD, peak - equity);
          curve.push({ ts: timestamp, equity });
          openPosition = null;
        }
        const qty = Math.max(1, Math.floor(equity * (riskPercent/100) / price));
        openPosition = { entry: price, direction: action === 'buy' ? 'long' : 'short', qty, ts: timestamp };
      } else if (action === 'exit' && openPosition) {
        const exitPrice = price;
        const pnl = overridePnl !== undefined ? overridePnl :
          (openPosition.direction === 'long' 
            ? (exitPrice - openPosition.entry) * openPosition.qty
            : (openPosition.entry - exitPrice) * openPosition.qty);
        equity += pnl;
        if (pnl > 0) { wins++; grossProfit += pnl; }
        else if (pnl < 0) { losses++; grossLoss += Math.abs(pnl); }
        peak = Math.max(peak, equity);
        maxDD = Math.max(maxDD, peak - equity);
        curve.push({ ts: timestamp, equity });
        openPosition = null;
      }
    }
    
    const totalTrades = wins + losses;
    const winRate = totalTrades ? (wins / totalTrades) * 100 : 0;
    const profitFactor = grossLoss ? grossProfit / grossLoss : (grossProfit > 0 ? Infinity : 0);
    const finalReturn = ((equity - startingEquity) / startingEquity) * 100;
    
    return json({
      startingEquity, finalEquity: equity, totalReturn: equity - startingEquity,
      returnPercent: finalReturn, maxDrawdown: maxDD, maxDrawdownPercent: (maxDD / peak) * 100,
      totalTrades, wins, losses, winRate, grossProfit, grossLoss, profitFactor, curve
    });
  }
  
  // GET /api/audit/export — compliance audit trail export
  if (path === '/api/audit/export' && request.method === 'GET') {
    const types = ['signal:', 'trade:', 'autoflat:', 'alert:'];
    const report = { generatedAt: new Date().toISOString(), entries: [] };
    for (const prefix of types) {
      const list = await kv.list({ prefix, limit: 1000 });
      for (const k of list.keys) {
        const item = await kv.get(k.name, 'json');
        if (item) report.entries.push({ type: prefix.replace(':',''), key: k.name, data: item });
      }
    }
    report.entries.sort((a, b) => (b.data.ts || b.data.timestamp || 0) - (a.data.ts || a.data.timestamp || 0));
    report.count = report.entries.length;
    return new Response(JSON.stringify(report, null, 2), {
      headers: { 'Content-Type': 'application/json', 'Content-Disposition': `attachment; filename="audit-${Date.now()}.json"`, ...CORS }
    });
  }
  
  // GET /api/analytics/performance — comprehensive performance metrics
  if (path === '/api/analytics/performance' && request.method === 'GET') {
    const tradesList = await kv.list({ prefix: 'trade:', limit: 1000 });
    const trades = [];
    for (const k of tradesList.keys) {
      const t = await kv.get(k.name, 'json');
      // Only count closed trades with actual pnl
      if (t && t.status === 'closed' && t.pnl !== null && t.pnl !== undefined) trades.push(t);
    }
    trades.sort((a, b) => (a.closedAt || a.ts || 0) - (b.closedAt || b.ts || 0));
    
    if (trades.length === 0) return json({ empty: true, trades: 0 });
    
    const wins = trades.filter(t => t.pnl > 0);
    const losses = trades.filter(t => t.pnl < 0);
    const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
    const grossProfit = wins.reduce((s, t) => s + t.pnl, 0);
    const grossLoss = Math.abs(losses.reduce((s, t) => s + t.pnl, 0));
    const winRate = wins.length / trades.length * 100;
    const avgWin = wins.length ? grossProfit / wins.length : 0;
    const avgLoss = losses.length ? grossLoss / losses.length : 0;
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Infinity : 0;
    const expectancy = (winRate / 100) * avgWin - ((100 - winRate) / 100) * avgLoss;
    
    // Equity curve + drawdown
    let equity = 0, peak = 0, maxDD = 0, maxDDPct = 0;
    const curve = [];
    for (const t of trades) {
      equity += t.pnl;
      peak = Math.max(peak, equity);
      const dd = peak - equity;
      if (dd > maxDD) { maxDD = dd; maxDDPct = peak > 0 ? (dd / peak) * 100 : 0; }
      curve.push({ ts: t.closedAt || t.ts, equity });
    }
    
    // Sharpe ratio (simplified, per-trade based): mean(return) / stddev(return) * sqrt(252)
    const returns = trades.map(t => t.pnl);
    const meanR = returns.reduce((s, r) => s + r, 0) / returns.length;
    const variance = returns.reduce((s, r) => s + Math.pow(r - meanR, 2), 0) / returns.length;
    const stddev = Math.sqrt(variance);
    const sharpe = stddev > 0 ? (meanR / stddev) * Math.sqrt(252) : 0;
    
    // Sortino: only downside stddev
    const downsideReturns = returns.filter(r => r < 0);
    const downsideVar = downsideReturns.length ? downsideReturns.reduce((s, r) => s + r * r, 0) / downsideReturns.length : 0;
    const downsideStd = Math.sqrt(downsideVar);
    const sortino = downsideStd > 0 ? (meanR / downsideStd) * Math.sqrt(252) : 0;
    
    // By day of week, by hour
    const byDow = [0,0,0,0,0,0,0];
    const byHour = new Array(24).fill(0);
    const byDowCount = [0,0,0,0,0,0,0];
    const byHourCount = new Array(24).fill(0);
    for (const t of trades) {
      const d = new Date(t.closedAt || t.ts);
      byDow[d.getUTCDay()] += t.pnl;
      byDowCount[d.getUTCDay()]++;
      byHour[d.getUTCHours()] += t.pnl;
      byHourCount[d.getUTCHours()]++;
    }
    
    // By ticker
    const byTicker = {};
    for (const t of trades) {
      const ticker = t.ticker || 'unknown';
      if (!byTicker[ticker]) byTicker[ticker] = { pnl: 0, count: 0, wins: 0 };
      byTicker[ticker].pnl += t.pnl;
      byTicker[ticker].count++;
      if (t.pnl > 0) byTicker[ticker].wins++;
    }
    
    // By strategy
    const byStrategy = {};
    for (const t of trades) {
      const sid = t.strategyId || 'unknown';
      if (!byStrategy[sid]) byStrategy[sid] = { pnl: 0, count: 0, wins: 0 };
      byStrategy[sid].pnl += t.pnl;
      byStrategy[sid].count++;
      if (t.pnl > 0) byStrategy[sid].wins++;
    }
    
    // Consecutive wins/losses streak
    let maxWinStreak = 0, maxLossStreak = 0, curW = 0, curL = 0;
    for (const t of trades) {
      if (t.pnl > 0) { curW++; curL = 0; maxWinStreak = Math.max(maxWinStreak, curW); }
      else { curL++; curW = 0; maxLossStreak = Math.max(maxLossStreak, curL); }
    }
    
    return json({
      totalTrades: trades.length, wins: wins.length, losses: losses.length,
      totalPnl, grossProfit, grossLoss, winRate, avgWin, avgLoss, profitFactor, expectancy,
      maxDD, maxDDPct, sharpe, sortino,
      maxWinStreak, maxLossStreak,
      curve, byDow, byDowCount, byHour, byHourCount, byTicker, byStrategy,
      firstTrade: trades[0]?.closedAt, lastTrade: trades[trades.length-1]?.closedAt
    });
  }
  
  // GET /api/diagnostic — end-to-end system test (doesn't place orders)
  if (path === '/api/diagnostic' && request.method === 'GET') {
    const tests = [];
    // Test 1: KV available
    try {
      await kv.put('diag:test', '1', { expirationTtl: 60 });
      const v = await kv.get('diag:test');
      tests.push({ name: 'KV Storage', ok: v === '1', detail: 'read/write OK' });
    } catch (e) {
      tests.push({ name: 'KV Storage', ok: false, detail: String(e) });
    }
    
    // Test 2: Connections can auth
    const connList = await kv.list({ prefix: 'connection:' });
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn) continue;
      const startMs = Date.now();
      try {
        const auth = await tvAuth(conn.env, conn);
        const latency = Date.now() - startMs;
        tests.push({ name: `Auth: ${conn.name}`, ok: !!auth.accessToken, detail: auth.accessToken ? `Token received in ${latency}ms` : `Failed: ${JSON.stringify(auth)}` });
        if (auth.accessToken) {
          // Test 3: List accounts
          const accRes = await tvApi(conn.env, '/account/list', auth.accessToken);
          tests.push({ name: `Accounts: ${conn.name}`, ok: accRes.ok, detail: accRes.ok ? `${(accRes.data || []).length} accounts found` : `Failed: ${accRes.error}` });
          // Test 4: List positions
          const posRes = await tvApi(conn.env, '/position/list', auth.accessToken);
          tests.push({ name: `Positions: ${conn.name}`, ok: posRes.ok, detail: posRes.ok ? `${(posRes.data || []).length} positions` : `Failed: ${posRes.error}` });
        }
      } catch (e) {
        tests.push({ name: `Auth: ${conn.name}`, ok: false, detail: String(e) });
      }
    }
    
    // Test 5: Strategies have valid webhooks
    const stratList = await kv.list({ prefix: 'strategy:' });
    for (const k of stratList.keys) {
      const s = await kv.get(k.name, 'json');
      if (!s) continue;
      const hasWebhook = !!(s.password && s.id);
      const hasSubs = (s.subscriptions || []).length > 0;
      tests.push({ name: `Strategy: ${s.name}`, ok: hasWebhook && hasSubs, detail: hasWebhook && hasSubs ? 'Webhook & subscriptions OK' : !hasWebhook ? 'Missing webhook password' : 'No subscriptions' });
    }
    
    // Test 6: Deadman config
    const deadmanConfig = await kv.get('deadman:config', 'json');
    tests.push({ name: 'Dead Man Switch', ok: !!deadmanConfig?.enabled, detail: deadmanConfig?.enabled ? `Armed, timeout ${deadmanConfig.timeoutSec}s` : 'Disarmed (recommended for Live trading)' });
    
    const totalOk = tests.filter(t => t.ok).length;
    return json({ ok: totalOk === tests.length, tests, totalOk, total: tests.length });
  }
  
  // ========== PER-STRATEGY DEEP ANALYTICS ==========
  // GET /api/analytics/strategy/:id — full per-strategy breakdown
  const saMatch = path.match(/^\/api\/analytics\/strategy\/([^/]+)$/);
  if (saMatch && request.method === 'GET') {
    const sid = saMatch[1];
    const tradesList = await kv.list({ prefix: 'trade:' + sid + ':', limit: 1000 });
    const trades = [];
    for (const k of tradesList.keys) {
      const t = await kv.get(k.name, 'json');
      if (t && t.status === 'closed' && typeof t.pnl === 'number') trades.push(t);
    }
    trades.sort((a, b) => (a.closedAt || a.ts || 0) - (b.closedAt || b.ts || 0));
    if (trades.length === 0) return json({ empty: true, strategyId: sid });
    const wins = trades.filter(t => t.pnl > 0);
    const losses = trades.filter(t => t.pnl < 0);
    const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
    const grossProfit = wins.reduce((s, t) => s + t.pnl, 0);
    const grossLoss = Math.abs(losses.reduce((s, t) => s + t.pnl, 0));
    const winRate = wins.length / trades.length * 100;
    const avgWin = wins.length ? grossProfit / wins.length : 0;
    const avgLoss = losses.length ? grossLoss / losses.length : 0;
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? 999 : 0;
    const expectancy = (winRate / 100) * avgWin - ((100 - winRate) / 100) * avgLoss;
    let eq = 0, peak = 0, maxDD = 0;
    const curve = [];
    for (const t of trades) { eq += t.pnl; peak = Math.max(peak, eq); const dd = peak - eq; if (dd > maxDD) maxDD = dd; curve.push({ ts: t.closedAt || t.ts, eq }); }
    // Win rate over time (rolling 10)
    const rollingWR = [];
    for (let i = 9; i < trades.length; i++) {
      const window = trades.slice(i - 9, i + 1);
      const w = window.filter(t => t.pnl > 0).length;
      rollingWR.push({ ts: window[window.length - 1].closedAt || window[window.length - 1].ts, wr: w * 10 });
    }
    // Hour heatmap (ET)
    const byHourET = new Array(24).fill(0).map(() => ({ pnl: 0, count: 0 }));
    for (const t of trades) {
      const ts = t.closedAt || t.ts;
      if (!ts) continue;
      const off = wGetETOffsetHours(new Date(ts));
      const hET = new Date(ts + off * 3600000).getUTCHours();
      byHourET[hET].pnl += t.pnl;
      byHourET[hET].count++;
    }
    let maxWS = 0, maxLS = 0, cW = 0, cL = 0;
    for (const t of trades) { if (t.pnl > 0) { cW++; cL = 0; maxWS = Math.max(maxWS, cW); } else { cL++; cW = 0; maxLS = Math.max(maxLS, cL); } }
    return json({ strategyId: sid, totalTrades: trades.length, wins: wins.length, losses: losses.length, totalPnl, grossProfit, grossLoss, winRate, avgWin, avgLoss, profitFactor, expectancy, maxDD, curve, rollingWR, byHourET, maxWS, maxLS, firstTrade: trades[0].closedAt || trades[0].ts, lastTrade: trades[trades.length - 1].closedAt || trades[trades.length - 1].ts });
  }

  // ========== LATENCY TIMELINE (5-min rolling sparkline) ==========
  // GET /api/latency/timeline — returns 60 data points over last 5 min (one per 5-sec bucket)
  if (path === '/api/latency/timeline' && request.method === 'GET') {
    const now = Date.now();
    const windowMs = 5 * 60 * 1000;
    const bucketMs = 5 * 1000;
    const buckets = Math.floor(windowMs / bucketMs);
    const series = new Array(buckets).fill(null);
    const sigList = await kv.list({ prefix: 'signal:', limit: 500 });
    for (const k of sigList.keys) {
      const s = await kv.get(k.name, 'json');
      if (!s || !s.timestamp) continue;
      const age = now - s.timestamp;
      if (age > windowMs || age < 0) continue;
      const idx = buckets - 1 - Math.floor(age / bucketMs);
      if (idx < 0 || idx >= buckets) continue;
      const latencies = [];
      if (s.result?.results) {
        for (const r of s.result.results) {
          if (typeof r.enterLatencyMs === 'number') latencies.push(r.enterLatencyMs);
        }
      }
      if (latencies.length) {
        const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
        series[idx] = series[idx] === null ? avg : (series[idx] + avg) / 2;
      }
    }
    return json({ series, bucketMs, windowMs, ts: now });
  }

  // ========== OCO ORDERS (One Cancels Other) ==========
  // POST /api/oco/create — pair 2 orders; if one fills, cancel the other
  if (path === '/api/oco/create' && request.method === 'POST') {
    const body = await request.json();
    const { connectionId, accountId, symbol, orderA, orderB } = body;
    const conn = await kv.get(`connection:${connectionId}`, 'json');
    if (!conn) return json({ error: 'conn-not-found' }, 404);
    const token = await getCachedToken(kv, conn);
    if (!token) return json({ error: 'auth-failed' }, 401);
    // Place both orders via Tradovate
    const rA = await tvApiWithAuth(kv, conn, '/order/placeorder', 'POST', { accountId, symbol, ...orderA });
    if (!rA.ok) return json({ error: 'order-a-failed', details: rA }, 400);
    const rB = await tvApiWithAuth(kv, conn, '/order/placeorder', 'POST', { accountId, symbol, ...orderB });
    if (!rB.ok) {
      // Rollback A
      await tvApiWithAuth(kv, conn, '/order/cancelorder', 'POST', { orderId: rA.data?.orderId });
      return json({ error: 'order-b-failed', details: rB }, 400);
    }
    const ocoId = 'oco_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    const record = {
      id: ocoId, connectionId, accountId, symbol,
      orderAId: rA.data?.orderId, orderBId: rB.data?.orderId,
      createdAt: Date.now(), status: 'active'
    };
    await kv.put(`oco:${ocoId}`, JSON.stringify(record), { expirationTtl: 86400 * 7 });
    return json({ ok: true, ocoId, orderAId: record.orderAId, orderBId: record.orderBId });
  }
  // POST /api/oco/cancel/:id — cancel remaining leg
  const ocoCancelMatch = path.match(/^\/api\/oco\/cancel\/([^/]+)$/);
  if (ocoCancelMatch && request.method === 'POST') {
    const rec = await kv.get(`oco:${ocoCancelMatch[1]}`, 'json');
    if (!rec) return json({ error: 'oco-not-found' }, 404);
    const conn = await kv.get(`connection:${rec.connectionId}`, 'json');
    if (!conn) return json({ error: 'conn-not-found' }, 404);
    await tvApiWithAuth(kv, conn, '/order/cancelorder', 'POST', { orderId: rec.orderAId }).catch(() => {});
    await tvApiWithAuth(kv, conn, '/order/cancelorder', 'POST', { orderId: rec.orderBId }).catch(() => {});
    rec.status = 'cancelled';
    rec.cancelledAt = Date.now();
    await kv.put(`oco:${rec.id}`, JSON.stringify(rec), { expirationTtl: 86400 * 7 });
    return json({ ok: true, ocoId: rec.id });
  }
  // GET /api/oco/list — active OCO pairs
  if (path === '/api/oco/list' && request.method === 'GET') {
    const list = await kv.list({ prefix: 'oco:' });
    const recs = await Promise.all(list.keys.map(k => kv.get(k.name, 'json')));
    return json({ ocos: recs.filter(Boolean).sort((a, b) => b.createdAt - a.createdAt) });
  }

  // ========== APEX CONSISTENCY PREVIEW ==========
  // GET /api/apex/consistency?connection=X — returns % of best day vs total, warns if >50%
  if (path === '/api/apex/consistency' && request.method === 'GET') {
    const cid = url.searchParams.get('connection');
    if (!cid) return json({ error: 'connection required' }, 400);
    const tradesList = await kv.list({ prefix: 'trade:', limit: 1000 });
    const trades = [];
    for (const k of tradesList.keys) {
      const t = await kv.get(k.name, 'json');
      if (t && t.connectionId === cid && t.status === 'closed' && typeof t.pnl === 'number') trades.push(t);
    }
    const dayMap = {};
    for (const t of trades) {
      const d = new Date(t.closedAt || t.ts).toISOString().slice(0, 10);
      dayMap[d] = (dayMap[d] || 0) + t.pnl;
    }
    const days = Object.entries(dayMap).filter(([, pnl]) => pnl > 0);
    const total = days.reduce((s, [, pnl]) => s + pnl, 0);
    const bestDay = days.length ? Math.max(...days.map(([, pnl]) => pnl)) : 0;
    const bestPct = total > 0 ? (bestDay / total) * 100 : 0;
    return json({ bestDay, total, bestPct, warning: bestPct >= 50, days: days.length, breakdown: dayMap });
  }

  return json({ error: 'not-found', path }, 404);
}

// Cloudflare Cron Trigger handler — runs on schedule defined in wrangler.toml
// Crontab: "55 20 * * 1-5" = 20:55 UTC Mon-Fri = 16:55 EDT (accurate during DST)
// For standard time (EST = UTC-5) use "55 21 * * 1-5"
// Apex closes 16:59 ET, we trigger at 16:55 ET for 4-min buffer
// ALSO runs deadman-switch check every minute ("* * * * *")
async function handleScheduled(event, env) {
  const kv = env.PROFITBOT_KV;
  const etNow = wGetETNow();
  
  // ========== TRADE CLOSURE TRACKER ==========
  // Poll positions for all connections every minute.
  // Compare with last snapshot; any position that disappeared or changed direction = closed.
  // Calculate realized P&L and update trade records accordingly.
  try {
    const connList = await kv.list({ prefix: 'connection:' });
    for (const k of connList.keys) {
      const conn = await kv.get(k.name, 'json');
      if (!conn || conn.paused) continue;
      const token = await getCachedToken(kv, conn);
      if (!token) continue;
      
      // Fetch current positions + fills for this connection
      const posRes = await tvApi(conn.env, '/position/list', token);
      const currentPositions = (posRes.data || []).filter(p => p.netPos !== undefined);
      
      // Load last snapshot
      const snapshotKey = `pos-snapshot:${conn.id}`;
      const lastSnapshot = await kv.get(snapshotKey, 'json') || { positions: [], ts: 0 };
      
      // Build a key for each position: accountId+symbol
      const posKey = p => `${p.accountId}:${p.contractName || p.symbol}`;
      const currentMap = new Map(currentPositions.map(p => [posKey(p), p]));
      const lastMap = new Map((lastSnapshot.positions || []).map(p => [posKey(p), p]));
      
      // Detect closures: full close OR partial close OR direction reversal
      for (const [key, lastPos] of lastMap) {
        if (lastPos.netPos === 0) continue; // Wasn't open
        const curPos = currentMap.get(key);
        const curQty = curPos?.netPos || 0;
        const lastQty = lastPos.netPos;
        
        // Calculate change in position
        const qtyDelta = lastQty - curQty; // positive = reduction in position
        if (qtyDelta === 0) continue; // No change
        if (Math.sign(curQty) === Math.sign(lastQty) && Math.abs(curQty) >= Math.abs(lastQty)) continue; // Added to position, not closed
        
        // Determine if this is full close, partial close, or reversal
        const isFullClose = curQty === 0;
        const isReversal = Math.sign(curQty) !== Math.sign(lastQty) && curQty !== 0;
        const isPartialClose = !isFullClose && !isReversal && Math.sign(curQty) === Math.sign(lastQty);
        
        // Amount that was closed in this update
        const closedQty = isReversal ? Math.abs(lastQty) : Math.abs(qtyDelta);
        if (closedQty === 0) continue;
        
        // Position closed. Fetch recent fills to get exit price.
        try {
          const fillsRes = await tvApi(conn.env, '/fill/list', token);
          const fills = (fillsRes.data || [])
            .filter(f => f.accountId === lastPos.accountId && f.contractName === (lastPos.contractName || lastPos.symbol))
            .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
          
          const exitFill = fills[0]; // most recent fill = the closing one
          if (!exitFill) continue;
          
          const exitPrice = exitFill.price;
          const exitTime = new Date(exitFill.timestamp).getTime();
          
          // Find the matching open trade record
          const tradesList = await kv.list({ prefix: 'trade-by-conn:' + conn.id + ':', limit: 100 });
          for (const tk of tradesList.keys) {
            const trade = await kv.get(tk.name, 'json');
            if (!trade || trade.status !== 'open') continue;
            if (trade.ticker !== (lastPos.contractName || lastPos.symbol)) continue;
            if (!trade.entryPrice) continue;
            
            // Calculate pnl: (exit - entry) × qty × pointValue × direction
            const isLong = String(trade.action).toLowerCase() === 'buy';
            const priceDiff = isLong ? (exitPrice - trade.entryPrice) : (trade.entryPrice - exitPrice);
            const pointValue = trade.pointValue || 1;
            // For partial: pnl scales with closedQty not full trade.quantity
            const pnl = priceDiff * Math.min(Math.abs(trade.quantity || 1), closedQty) * pointValue;
            
            // Update trade record
            if (isFullClose || isReversal || closedQty >= Math.abs(trade.quantity || 1)) {
              trade.status = 'closed';
              trade.exitPrice = exitPrice;
              trade.exitTime = exitTime;
              trade.closedAt = exitTime;
              trade.pnl = pnl;
            } else if (isPartialClose) {
              // Partial: track partials array and update remaining qty
              trade.partials = trade.partials || [];
              trade.partials.push({ exitPrice, exitTime, closedQty, pnl });
              trade.remainingQty = Math.abs(curQty);
              // Accumulate partial pnl but keep status open
              trade.pnl = (trade.pnl || 0) + pnl;
            }
            
            await kv.put(tk.name, JSON.stringify(trade));
            // Also update main trade: key
            await kv.put(`trade:${trade.strategyId}:${trade.ts}:${trade.id}`, JSON.stringify(trade), { expirationTtl: 2592000 });
            
            // Fire notification for fully-closed trades only (not partials to avoid spam)
            if (trade.status === 'closed') {
              const strat = await kv.get(`strategy:${trade.strategyId}`, 'json');
              notifyChannels(kv, 'trade', {
                ticker: trade.ticker, action: trade.action, pnl: trade.pnl,
                strategy: strat?.name || trade.strategyId
              }).catch(() => {});
            }
            
            break; // one trade per closure
          }
        } catch (e) { /* per-symbol fail is okay, keep going */ }
      }
      
      // Save new snapshot
      await kv.put(snapshotKey, JSON.stringify({ positions: currentPositions, ts: Date.now() }), { expirationTtl: 86400 });
    }
  } catch (e) {
    await kv.put('alert:closure-tracker-error:' + Date.now(), JSON.stringify({ ts: Date.now(), error: String(e) }), { expirationTtl: 86400 });
  }
  
  // ========== DEAD MAN SWITCH CHECK ==========
  // Runs every invocation. If deadman is armed and client hasn't pinged in timeoutSec,
  // auto-flat everything as a safety fallback.
  try {
    const deadmanConfig = await kv.get('deadman:config', 'json');
    if (deadmanConfig?.enabled) {
      const lastPing = await kv.get('deadman:lastping');
      const stalenessMs = lastPing ? Date.now() - parseInt(lastPing) : Infinity;
      if (stalenessMs > deadmanConfig.timeoutSec * 1000) {
        // CRITICAL: client is dead. Flat everything NOW.
        await kv.put('alert:deadman-triggered:' + Date.now(), JSON.stringify({ ts: Date.now(), stalenessMs, timeoutSec: deadmanConfig.timeoutSec }), { expirationTtl: 86400 * 7 });
        const connList = await kv.list({ prefix: 'connection:' });
        let totalClosed = 0;
        for (const k of connList.keys) {
          const conn = await kv.get(k.name, 'json');
          if (!conn || conn.paused) continue;
          const token = await getCachedToken(kv, conn);
          if (!token) continue;
          const posRes = await tvApi(conn.env, '/position/list', token);
          const positions = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
          for (const p of positions) {
            const r = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', { accountId: p.accountId, symbol: p.contractName || p.symbol, admin: false });
            if (r.ok) totalClosed++;
          }
        }
        await kv.put('autoflat:' + Date.now(), JSON.stringify({ ts: Date.now(), reason: 'deadman-switch', totalClosed }), { expirationTtl: 86400 * 30 });
        // Disarm to prevent loop
        await kv.delete('deadman:config');
      }
    }
  } catch (e) { /* continue to 16:55 check */ }
  
  // ========== 16:55 ET AUTO-FLAT CHECK ==========
  // Also check exchange calendar — if early close day, fire at 12:55 ET instead of 16:55
  let flatTriggerTime = 16 * 60 + 55; // default
  try {
    const todayStr = new Date().toISOString().slice(0, 10);
    // Inline calendar check for early-close days
    const earlyCloseDates = ['2026-07-03', '2026-11-27', '2026-12-24', '2026-12-31'];
    if (earlyCloseDates.includes(todayStr)) flatTriggerTime = 12 * 60 + 55;
    const fullCloseDates = ['2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-04', '2026-09-07', '2026-11-26', '2026-12-25'];
    if (fullCloseDates.includes(todayStr)) return; // Market closed, nothing to flatten
  } catch (e) { /* use default */ }
  
  // Only fire within the window, AND only once per day
  if (etNow.totalMin < flatTriggerTime || etNow.totalMin > flatTriggerTime + 5) {
    return;
  }
  // Check if auto-flat already ran today (prevent duplicate runs)
  const todayStr = new Date().toISOString().slice(0, 10);
  const runKey = 'autoflat-ran:' + todayStr;
  if (await kv.get(runKey)) return; // Already ran today
  // Only fire on weekdays
  const day = new Date().getUTCDay();
  if (day === 0 || day === 6) return;
  // Mark as run
  await kv.put(runKey, String(Date.now()), { expirationTtl: 86400 });
  
  // Execute auto-flat
  const connList = await kv.list({ prefix: 'connection:' });
  const results = [];
  let totalClosed = 0;
  for (const k of connList.keys) {
    const conn = await kv.get(k.name, 'json');
    if (!conn || conn.paused) continue;
    const token = await getCachedToken(kv, conn);
    if (!token) continue;
    const posRes = await tvApi(conn.env, '/position/list', token);
    const positions = (posRes.data || []).filter(p => p.netPos && p.netPos !== 0);
    for (const p of positions) {
      const ordersRes = await tvApi(conn.env, '/order/list', token);
      const pending = (ordersRes.data || []).filter(o => o.accountId === p.accountId && ['Working', 'Pending'].includes(o.ordStatus));
      for (const ord of pending) {
        await tvApi(conn.env, '/order/cancelorder', token, 'POST', { orderId: ord.id });
      }
      const r = await tvApi(conn.env, '/order/liquidatePosition', token, 'POST', {
        accountId: p.accountId, symbol: p.contractName || p.symbol, admin: false
      });
      if (r.ok) totalClosed++;
      results.push({ conn: conn.id, symbol: p.contractName || p.symbol, ok: r.ok });
    }
  }
  await kv.put('autoflat:' + Date.now(), JSON.stringify({ ts: Date.now(), reason: flatTriggerTime < 16*60 ? 'cron-early-close' : 'cron-1655-et', totalClosed, results }), { expirationTtl: 86400 * 30 });
  // Notify all configured channels about auto-flat
  notifyChannels(kv, 'autoflat', { reason: flatTriggerTime < 16*60 ? 'Early-close day' : '16:55 ET cutoff', totalClosed, totalFailed: results.filter(r => !r.ok).length }).catch(() => {});
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const whMatch = path.match(/^\/trading\/webhook\/([^/]+)\/([^/]+)$/);
    if (whMatch) return handleWebhook(request, env, whMatch[1], whMatch[2]);
    if (path.startsWith('/api/')) return handleApi(request, env, path);
    if (path === '/' || path === '/health') {
      return json({ service: 'ProfitBot Terminal Worker', status: 'ok', version: '2.1' });
    }
    return json({ error: 'not-found' }, 404);
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(handleScheduled(event, env));
  }
};
