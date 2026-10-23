import fs from 'fs';
import path from 'path';

const EXCHANGES = {
  binance: { name: "Binance", base: "https://fapi1.binance.com" },
  okx: { name: "OKX", base: "https://www.okx.com" },
  mexc: { name: "MEXC", base: "https://contract.mexc.com" },
};

const CONFIG = {
  TIMEFRAMES: ["1h", "30m", "15m"],
  INTERVAL_MS: { "15m": 15 * 60 * 1000, "30m": 30 * 60 * 1000, "1h": 60 * 60 * 1000 },
  FETCH_CANDLES: 130,
  MIN_CANDLES_REQUIRED: 30,
  TRADES_FILE: path.resolve('src', 'trades.json'),

  TOP_SYMBOL_COUNT: 9999999999,
  MIN_QUOTE_VOLUME_24H: 20000000,
  ANALYSIS_ORDER: ["binance", "okx", "mexc"],

  API_MAX_RETRIES: 3,
  REQUEST_TIMEOUT_MS: 15000,
  MESSAGE_DELAY_MS: 300,
  TOTAL_TIMEOUT_MS: 12 * 60 * 1000,
  CONCURRENCY: 5,

  DOJI_MAX_BODY_RATIO: 0.10,
  WICK_SYMMETRY_TOLERANCE: 0.15,
  CONFIRMATION_MIN_BODY_RATIO: 0.30,

  SL_BUFFER_PERCENT: 0,
  SL_ATR_MULTIPLIER: 1.2,
  MIN_RR: 1.5,
  MAX_RR: 4,

  USE_CONFIRMATION_ATR_FILTERS: true,
  USE_SMA_FILTER: true,
  USE_RANGE_POSITION_FILTER: true,
};

const klineCache = {};
const KLINE_CACHE_TTL_MS = 15 * 1000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function loadTrades() {
  try {
    if (fs.existsSync(CONFIG.TRADES_FILE)) {
      return JSON.parse(fs.readFileSync(CONFIG.TRADES_FILE, 'utf8'));
    }
  } catch (e) {}
  return [];
}

function saveTrades(trades) {
  try {
    fs.writeFileSync(CONFIG.TRADES_FILE, JSON.stringify(trades, null, 2), 'utf8');
  } catch (e) {}
}

async function fetchWithRetry(url, exchangeName, validateJson) {
  let lastError = null;

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), CONFIG.REQUEST_TIMEOUT_MS);

      let response;
      try {
        response = await fetch(url, {
          headers: { Accept: "application/json" },
          signal: controller.signal,
        });
      } finally {
        clearTimeout(timeout);
      }

      if (response.status === 429) {
        const retryAfter = parseFloat(response.headers.get("Retry-After") || "0");
        await sleep(retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1));
        continue;
      }

      if (!response.ok) throw new Error(`${exchangeName} HTTP ${response.status}`);

      const json = await response.json();
      validateJson(json);
      return json;
    } catch (error) {
      lastError = error;
      if (attempt < CONFIG.API_MAX_RETRIES - 1) await sleep(500 * (attempt + 1));
    }
  }
  throw lastError || new Error(`${exchangeName} failed after retries`);
}

function createExchangeFetcher(baseUrl, exchangeName, validator) {
  return async function fetchApi(path, params = {}) {
    const url = new URL(baseUrl + path);
    for (const [key, value] of Object.entries(params)) {
      url.searchParams.set(key, String(value));
    }
    return fetchWithRetry(url.toString(), exchangeName, validator);
  };
}

const binanceFetch = createExchangeFetcher(EXCHANGES.binance.base, "Binance", (json) => {
  if (json == null || (Array.isArray(json) && json.length === 0)) throw new Error(`Binance: invalid response`);
});

const okxFetch = createExchangeFetcher(EXCHANGES.okx.base, "OKX", (json) => {
  if (!json || json.code !== "0") throw new Error(`OKX: ${json?.msg || "unknown error"}`);
});

const mexcFetch = createExchangeFetcher(EXCHANGES.mexc.base, "MEXC", (json) => {
  if (json == null || (json.success !== undefined && !json.success)) throw new Error(`MEXC: invalid response`);
});

async function fetchBinanceSymbols() {
  const json = await binanceFetch("/fapi/v1/exchangeInfo");
  return (json.symbols || [])
    .filter((item) => item.status === "TRADING" && item.quoteAsset === "USDT" && item.contractType === "PERPETUAL")
    .map((item) => ({ symbol: item.symbol, base: item.baseAsset, exchange: "binance", raw: item.symbol }));
}

async function fetchBinanceTickers() {
  const json = await binanceFetch("/fapi/v1/ticker/24hr");
  return json
    .filter((item) => item.symbol.endsWith("USDT"))
    .map((item) => ({ symbol: item.symbol, volume: parseFloat(item.quoteVolume || "0") || 0 }));
}

async function fetchOKXSymbols() {
  const json = await okxFetch("/api/v5/public/instruments", { instType: "SWAP" });
  return (json.data || [])
    .filter((item) => item.state === "live" && item.settleCcy === "USDT" && item.ctType === "linear" && item.instId.endsWith("-USDT-SWAP"))
    .map((item) => {
      const base = item.instId.split("-")[0];
      return { symbol: `${base}USDT`, base, exchange: "okx", raw: item.instId };
    });
}

async function fetchOKXTickers() {
  const json = await okxFetch("/api/v5/market/tickers", { instType: "SWAP" });
  return (json.data || [])
    .filter((item) => item.instId.endsWith("-USDT-SWAP"))
    .map((item) => {
      const base = item.instId.split("-")[0];
      return { symbol: `${base}USDT`, volume: parseFloat(item.volCcy24h || "0") || 0 };
    });
}

async function fetchMEXCSymbols() {
  const json = await mexcFetch("/api/v1/contract/detail");
  return (json.data || [])
    .filter((item) => item.state === 0 && item.quoteCoin === "USDT" && item.settleCoin === "USDT")
    .map((item) => ({ symbol: item.symbol, base: item.baseCoin, exchange: "mexc", raw: item.symbol }));
}

async function fetchMEXCTickers() {
  const json = await mexcFetch("/api/v1/contract/ticker");
  return (json.data || [])
    .filter((item) => item.symbol.endsWith("_USDT"))
    .map((item) => ({ symbol: item.symbol.replace("_USDT", "USDT"), volume: parseFloat(item.amount24 || "0") || 0 }));
}

async function getTopSymbols() {
  const allCandidates = [];
  const exchangeSources = [
    { name: "binance", fetchSymbols: fetchBinanceSymbols, fetchTickers: fetchBinanceTickers },
    { name: "okx", fetchSymbols: fetchOKXSymbols, fetchTickers: fetchOKXTickers },
    { name: "mexc", fetchSymbols: fetchMEXCSymbols, fetchTickers: fetchMEXCTickers },
  ];

  const orderedSources = CONFIG.ANALYSIS_ORDER.map((name) => exchangeSources.find((ex) => ex.name === name)).filter(Boolean);

  for (const exchange of orderedSources) {
    try {
      const [symbols, tickers] = await Promise.all([exchange.fetchSymbols(), exchange.fetchTickers()]);
      if (!symbols || symbols.length === 0) continue;

      const volumeMap = new Map(tickers.map((t) => [t.symbol, t.volume]));
      const ranked = symbols
        .map((s) => ({ ...s, volume: volumeMap.get(s.symbol) || 0 }))
        .filter((s) => s.volume >= CONFIG.MIN_QUOTE_VOLUME_24H)
        .sort((a, b) => b.volume - a.volume);

      allCandidates.push(...ranked);
    } catch (error) {}
  }

  if (allCandidates.length === 0) throw new Error("No symbols passed volume filter.");

  const uniqueMap = new Map();
  for (const item of allCandidates) {
    const existing = uniqueMap.get(item.symbol);
    if (!existing || item.volume > existing.volume) uniqueMap.set(item.symbol, item);
  }

  return [...uniqueMap.values()].sort((a, b) => b.volume - a.volume).slice(0, CONFIG.TOP_SYMBOL_COUNT);
}

function isValidCandle(c) {
  return Number.isFinite(c.time) && Number.isFinite(c.open) && Number.isFinite(c.high) && Number.isFinite(c.low) && Number.isFinite(c.close) && c.high >= c.low && c.high >= Math.max(c.open, c.close) && c.low <= Math.min(c.open, c.close) && c.low > 0;
}

function getMexcInterval(tf) {
  if (tf === "1h") return "Min60";
  if (tf === "30m") return "Min30";
  return "Min15";
}

async function getCandles(symbol, exchange, timeframe) {
  const cacheKey = `${symbol}:${exchange}:${timeframe}`;
  const cached = klineCache[cacheKey];

  if (cached && Date.now() - cached.time < KLINE_CACHE_TTL_MS) {
    return cached.candles;
  }

  const base = symbol.replace(/USDT$/, "");
  const exchangesToTry = [exchange, ...CONFIG.ANALYSIS_ORDER.filter((e) => e !== exchange)];
  let lastError = null;

  for (const currentExchange of exchangesToTry) {
    try {
      let candles = [];
      const intervalMs = CONFIG.INTERVAL_MS[timeframe];

      if (currentExchange === "binance") {
        const json = await binanceFetch("/fapi/v1/klines", { symbol, interval: timeframe, limit: String(CONFIG.FETCH_CANDLES) });
        if (!Array.isArray(json)) throw new Error("Invalid Binance");
        candles = json.map((row) => ({ time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), closed: Number(row[6]) <= Date.now() }));
      } else if (currentExchange === "okx") {
        const json = await okxFetch("/api/v5/market/candles", { instId: `${base}-USDT-SWAP`, bar: timeframe, limit: String(CONFIG.FETCH_CANDLES) });
        if (!Array.isArray(json.data)) throw new Error("Invalid OKX");
        candles = json.data.map((row) => ({ time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), closed: (Number(row[0]) + intervalMs) <= Date.now() }));
      } else if (currentExchange === "mexc") {
        const json = await mexcFetch("/api/v1/contract/kline/" + symbol, { interval: getMexcInterval(timeframe) });
        if (!json.data || !Array.isArray(json.data.time)) throw new Error("Invalid MEXC");
        const { time, open, high, low, close } = json.data;
        candles = time.map((t, i) => ({ time: Number(t) * 1000, open: Number(open[i]), high: Number(high[i]), low: Number(low[i]), close: Number(close[i]), closed: (Number(t) * 1000) + intervalMs <= Date.now() }));
      }

      candles.sort((a, b) => a.time - b.time);
      const closed = candles.filter((c) => c.closed && isValidCandle(c));
      const unique = [];
      const seen = new Set();
      for (const c of closed) {
        if (!seen.has(c.time)) { seen.add(c.time); unique.push(c); }
      }

      if (unique.length >= CONFIG.MIN_CANDLES_REQUIRED) {
        const result = unique.slice(-CONFIG.FETCH_CANDLES);
        klineCache[cacheKey] = { time: Date.now(), candles: result };
        return result;
      }
      throw new Error(`Only ${unique.length} candles`);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("No candles");
}

function getDojiType(candle) {
  const range = candle.high - candle.low;
  if (!Number.isFinite(range) || range <= 0) return null;
  const body = Math.abs(candle.close - candle.open);
  if (body / range > CONFIG.DOJI_MAX_BODY_RATIO) return null;
  const upperWick = candle.high - Math.max(candle.open, candle.close);
  const lowerWick = Math.min(candle.open, candle.close) - candle.low;
  if (upperWick < 0 || lowerWick < 0 || upperWick === 0 || lowerWick === 0) return null;
  const wickSum = upperWick + lowerWick;
  if (wickSum <= 0) return null;
  if (Math.abs(upperWick - lowerWick) / wickSum <= CONFIG.WICK_SYMMETRY_TOLERANCE) return "standard";
  return null;
}

function calculateATR(candles, period = 14) {
  if (!candles || candles.length < period + 1) return 0;
  const trs = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    if (i === 0) trs.push(c.high - c.low);
    else {
      const pc = candles[i - 1].close;
      trs.push(Math.max(c.high - c.low, Math.abs(c.high - pc), Math.abs(c.low - pc)));
    }
  }
  return trs.slice(-period).reduce((s, v) => s + v, 0) / period;
}

function applyTechnicalFilters(candles, dojiIndex, direction, doji, confirmation, atrValue, sma50) {
  const confirmationRange = confirmation.high - confirmation.low;
  const confirmationBody = Math.abs(confirmation.close - confirmation.open);
  const dojiRange = doji.high - doji.low;

  if (confirmationBody < dojiRange * CONFIG.CONFIRMATION_MIN_BODY_RATIO) return false;

  if (CONFIG.USE_CONFIRMATION_ATR_FILTERS) {
    if (confirmationRange < atrValue * 0.5) return false;
    if (confirmationBody < atrValue * 0.3) return false;
    const breakoutDistance = direction === "LONG" ? confirmation.close - doji.high : doji.low - confirmation.close;
    if (breakoutDistance > atrValue * 1.5) return false;
  }

  if (CONFIG.USE_SMA_FILTER) {
    if (direction === "LONG" && doji.close < sma50) return false;
    if (direction === "SHORT" && doji.close > sma50) return false;
  }

  if (CONFIG.USE_RANGE_POSITION_FILTER) {
    const lookbackStart = Math.max(0, dojiIndex - 100);
    let recentHigh = -Infinity;
    let recentLow = Infinity;

    for (let i = lookbackStart; i < dojiIndex; i++) {
      recentHigh = Math.max(recentHigh, candles[i].high);
      recentLow = Math.min(recentLow, candles[i].low);
    }

    if (!Number.isFinite(recentHigh) || !Number.isFinite(recentLow)) return false;
    const range100 = recentHigh - recentLow;
    if (range100 <= 0) return false;

    const dojiMid = (doji.high + doji.low) / 2;
    const distanceFromHigh = (recentHigh - doji.high) / range100;

    if (direction === "LONG") {
      if (distanceFromHigh < 0.20) return false;
      if (doji.low > recentLow + range100 * 0.35) return false;
      if (dojiMid > (recentHigh + recentLow) / 2) return false;
    } else {
      if (distanceFromHigh < 0.20) return false;
      if (doji.high < recentHigh - range100 * 0.35) return false;
      if (doji.low < recentLow * 1.002) return false;
      if (dojiMid < (recentHigh + recentLow) / 2) return false;
    }
  }
  return true;
}

function analyzeSymbol(candles) {
  if (!Array.isArray(candles) || candles.length < CONFIG.MIN_CANDLES_REQUIRED) return null;
  const atrValue = calculateATR(candles);
  if (!Number.isFinite(atrValue) || atrValue <= 0) return null;

  const confirmationIndex = candles.length - 1;
  const dojiIndex = confirmationIndex - 1;
  const doji = candles[dojiIndex];
  const confirmation = candles[confirmationIndex];

  const kind = getDojiType(doji);
  if (!kind) return null;

  let direction = null;
  if (confirmation.close > doji.high) direction = "LONG";
  else if (confirmation.close < doji.low) direction = "SHORT";
  else return null;

  const dojiRange = doji.high - doji.low;
  if (!Number.isFinite(dojiRange) || dojiRange <= 0) return null;

  const sma50Period = Math.min(50, candles.length);
  const sma50 = candles.slice(-sma50Period).reduce((s, c) => s + c.close, 0) / sma50Period;

  if (!applyTechnicalFilters(candles, dojiIndex, direction, doji, confirmation, atrValue, sma50)) return null;

  const entry = confirmation.close;
  const buffer = Math.max(dojiRange * CONFIG.SL_BUFFER_PERCENT, atrValue * CONFIG.SL_ATR_MULTIPLIER);
  const sl = direction === "LONG" ? doji.low - buffer : doji.high + buffer;
  const risk = Math.abs(entry - sl);
  if (!Number.isFinite(risk) || risk <= 0) return null;

  const lookbackStart = Math.max(0, dojiIndex - 100);
  const structuralLevels = [];

  for (let i = lookbackStart; i < dojiIndex; i++) {
    const level = direction === "LONG" ? candles[i].high : candles[i].low;
    if (direction === "LONG" && level > entry) structuralLevels.push(level);
    if (direction === "SHORT" && level < entry) structuralLevels.push(level);
  }

  let tp = direction === "LONG" ? entry + risk * CONFIG.MAX_RR : entry - risk * CONFIG.MAX_RR;
  const validLevels = structuralLevels.filter((level) => {
    const rr = direction === "LONG" ? (level - entry) / risk : (entry - level) / risk;
    return rr >= CONFIG.MIN_RR && rr <= CONFIG.MAX_RR;
  });

  if (validLevels.length > 0) {
    validLevels.sort((a, b) => direction === "LONG" ? a - b : b - a);
    tp = validLevels[0];
  }

  const reward = direction === "LONG" ? tp - entry : entry - tp;
  const rr = reward / risk;

  if (!Number.isFinite(reward) || reward <= 0 || rr < CONFIG.MIN_RR || rr > CONFIG.MAX_RR) {
    tp = direction === "LONG" ? entry + risk * CONFIG.MIN_RR : entry - risk * CONFIG.MIN_RR;
  }

  const finalRR = direction === "LONG" ? (tp - entry) / risk : (entry - tp) / risk;

  return {
    direction,
    kind,
    entry,
    sl,
    risk,
    tp,
    rr: finalRR,
    dojiTime: doji.time,
    confirmationTime: confirmation.time,
    currentPrice: confirmation.close,
  };
}

function fmtPrice(value) {
  return Number.isFinite(value) ? value.toLocaleString("en-US", { maximumFractionDigits: 10, useGrouping: false }) : "N/A";
}

function fmtTime(timestamp) {
  return new Date(timestamp).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function escapeHtml(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function calcProfitPercent(entry, targetPrice) {
  if (!Number.isFinite(entry) || !Number.isFinite(targetPrice) || entry <= 0) return 0;
  return Math.abs((targetPrice - entry) / entry) * 100;
}

function formatSignal(symbol, signal, timeframe) {
  const isLong = signal.direction === "LONG";
  const dIcon = isLong ? "🟢" : "🔴";
  const dText = isLong ? "LONG" : "SHORT";

  return (
    "📊 <b>تحلیل Doji</b>\n\n" +
    `${dIcon} <b>${escapeHtml(symbol)}</b>\n` +
    `⏱ تایم‌فریم: ${timeframe}\n` +
    `📌 وضعیت: ${dIcon} <b>${dText}</b>\n\n` +
    `💰 قیمت فعلی: <b>${fmtPrice(signal.currentPrice)}</b>\n\n` +
    "🎯 <b>Doji Setup فعال:</b>\n" +
    `🕯 نوع دوجی: <b>Standard</b>\n` +
    `🎯 ورود: <b>${fmtPrice(signal.entry)}</b>\n` +
    `🛑 حد ضرر: <b>${fmtPrice(signal.sl)}</b>\n` +
    `📏 ریسک: <b>${fmtPrice(signal.risk)}</b>\n\n` +
    `🥇 TP: <b>${fmtPrice(signal.tp)}</b>\n` +
    `⚖️ R:R: <b>1:${signal.rr.toFixed(2)}</b>\n\n` +
    `⏳ <b>نتیجه: معامله باز است</b>\n` +
    `🕯 زمان تأیید: ${fmtTime(signal.confirmationTime)}\n\n` +
    "━━━━━━━━━━━━━━━━━━━━━━\n" +
    "💡 مدیریت ریسک و سرمایه:\n" +
    "🔸 با تاچ شدن TP، حد ضرر را به Breakeven منتقل کنید.\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
    "⚠️ این خروجی تحلیل الگوریتمی است و تضمین سود نیست."
  );
}

function formatTradeClosed(trade) {
  const isLong = trade.direction === "LONG";
  const dIcon = isLong ? "🟢" : "🔴";
  const dText = isLong ? "LONG" : "SHORT";
  const isWin = trade.status === "TP_HIT";
  
  const title = isWin ? "🏁 <b>معامله با موفقیت بسته شد</b>" : "🛑 <b>معامله با ضرر بسته شد</b>";
  const reasonText = isWin ? "✅ تارگت زده شد" : "❌ حد ضرر فعال شد";
  
  const hitPrice = isWin ? trade.tp : trade.sl;
  const profitPct = calcProfitPercent(trade.entry, hitPrice);
  const profitSign = isWin ? "+" : "-";
  const profitColor = isWin ? "🟢" : "🔴";

  return (
    title + "\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n" +
    `🔸 ارز: <b>${escapeHtml(trade.symbol)}</b>\n` +
    `⏱ تایم‌فریم: ${trade.timeframe}\n` +
    `📌 جهت: ${dIcon} <b>${dText}</b>\n\n` +
    `${reasonText}\n` +
    `💰 سود/ضرر خالص (بدون اهرم): <b>${profitColor} ${profitSign}${profitPct.toFixed(2)}%</b>\n\n` +
    `🎯 ورود: <b>${fmtPrice(trade.entry)}</b>\n` +
    `🏁 خروج: <b>${fmtPrice(hitPrice)}</b>\n\n` +
    "💡 مدیریت سرمایه را رعایت کنید. منتظر سیگنال بعدی باشید."
  );
}

function formatReversalWarning(symbol, oldTrade, newSignal, timeframe) {
  const oldDir = oldTrade.direction === "LONG" ? "🟢 LONG" : "🔴 SHORT";
  const newDir = newSignal.direction === "LONG" ? "🟢 LONG" : "🔴 SHORT";
  return (
    "⚠️ <b>هشدار تغییر روند!</b>\n\n" +
    `🔸 ارز: <b>${escapeHtml(symbol)}</b>\n` +
    `⏱ تایم‌فریم: ${timeframe}\n\n` +
    `❌ معامله قبلی شما (${oldDir}) هنوز باز است.\n` +
    `🔄 سیگنال جدید مخالف (${newDir}) شناسایی شد.\n\n` +
    "💡 <b>پیشنهاد:</b> لطفاً معامله قبلی را ببندید و در صورت تمایل معامله جدید را باز کنید.\n\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n"
  );
}

async function sendTelegram(token, chatId, message) {
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text: message, parse_mode: "HTML", disable_web_page_preview: true }),
  });
  const data = await res.json();
  if (!res.ok || !data?.ok) throw new Error(data?.description || `Telegram error`);
}

async function runWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runner() {
    while (true) {
      const currentIndex = nextIndex++;
      if (currentIndex >= items.length) return;
      try {
        results[currentIndex] = await worker(items[currentIndex], currentIndex);
      } catch (e) {
        results[currentIndex] = { error: e };
      }
    }
  }

  const runners = [];
  for (let i = 0; i < Math.min(concurrency, items.length); i++) {
    runners.push(runner());
  }

  await Promise.all(runners);
  return results;
}

async function checkOpenTrades(openTrades, token, chatId) {
  if (openTrades.length === 0) return [];
  
  console.log(`Checking ${openTrades.length} open trades...`);
  
  const tasks = openTrades.map((trade) => ({ trade }));
  
  const results = await runWithConcurrency(tasks, CONFIG.CONCURRENCY, async ({ trade }) => {
    try {
      const candles = await getCandles(trade.symbol, trade.exchange, trade.timeframe);
      
      const sentTime = Number(trade.confirmationTime);
      const signalCandles = candles.filter(c => c.time >= sentTime);
      
      if (signalCandles.length === 0 && candles.length > 0) {
        signalCandles.push(candles[candles.length - 1]);
      }

      let stopHit = false;
      let tpHit = false;
      let stopIndex = -1;
      let tpIndex = -1;

      for (let i = 0; i < signalCandles.length; i++) {
        const c = signalCandles[i];
        if (trade.direction === "LONG") {
          if (Number.isFinite(c.low) && c.low <= trade.sl && stopIndex === -1) { stopHit = true; stopIndex = i; }
          if (Number.isFinite(c.high) && c.high >= trade.tp && tpIndex === -1) { tpHit = true; tpIndex = i; }
        } else {
          if (Number.isFinite(c.high) && c.high >= trade.sl && stopIndex === -1) { stopHit = true; stopIndex = i; }
          if (Number.isFinite(c.low) && c.low <= trade.tp && tpIndex === -1) { tpHit = true; tpIndex = i; }
        }
      }

      const currentPrice = candles[candles.length - 1].close;
      if (trade.direction === "LONG") {
        if (currentPrice <= trade.sl && stopIndex === -1) { stopHit = true; stopIndex = signalCandles.length; }
        if (currentPrice >= trade.tp && tpIndex === -1) { tpHit = true; tpIndex = signalCandles.length; }
      } else {
        if (currentPrice >= trade.sl && stopIndex === -1) { stopHit = true; stopIndex = signalCandles.length; }
        if (currentPrice <= trade.tp && tpIndex === -1) { tpHit = true; tpIndex = signalCandles.length; }
      }

      let closedStatus = null;
      if (tpHit && stopHit) {
        closedStatus = tpIndex < stopIndex ? "TP_HIT" : "SL_HIT";
      } else if (tpHit) {
        closedStatus = "TP_HIT";
      } else if (stopHit) {
        closedStatus = "SL_HIT";
      }

      if (closedStatus) {
        trade.status = closedStatus;
        return { trade, closedStatus };
      } else {
        return { trade, closedStatus: null };
      }
    } catch (error) {
      console.error(`Failed to check ${trade.symbol}: ${error.message}`);
      return { trade, closedStatus: null };
    }
  });

  const remainingTrades = [];

  for (const result of results) {
    if (!result) continue;
    const { trade, closedStatus } = result;

    if (closedStatus) {
      const message = formatTradeClosed(trade);
      try {
        await sendTelegram(token, chatId, message);
        console.log(`Trade closed: ${trade.symbol} (${trade.timeframe}) -> ${closedStatus}`);
      } catch (e) {
        console.error(`Failed to send closure msg for ${trade.symbol}: ${e.message}`);
      }
    } else {
      remainingTrades.push(trade);
    }
  }

  return remainingTrades;
}

async function main() {
  const startTime = Date.now();
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID");
    process.exit(1);
  }

  console.log("Starting Doji scan...");

  let openTrades = loadTrades();
  openTrades = await checkOpenTrades(openTrades, token, chatId);

  let symbols = [];
  try {
    symbols = await getTopSymbols();
  } catch (error) {
    console.error("Failed to fetch symbols:", error.message);
    saveTrades(openTrades);
    process.exit(1);
  }

  console.log(`Fetched ${symbols.length} symbols to analyze.`);

  const tasks = [];
  for (const item of symbols) {
    tasks.push({ item });
  }

  let signalCount = 0;

  const scanResults = await runWithConcurrency(tasks, CONFIG.CONCURRENCY, async ({ item }) => {
    if (Date.now() - startTime > CONFIG.TOTAL_TIMEOUT_MS) {
      return { item, status: "timeout" };
    }

    const existingTrades = openTrades.filter(t => t.symbol === item.symbol);
    const existingTrade = existingTrades.length > 0 ? existingTrades[0] : null;

    for (const tf of CONFIG.TIMEFRAMES) {
      try {
        const candles = await getCandles(item.symbol, item.exchange, tf);
        const signal = analyzeSymbol(candles);

        if (signal) {
          const isSameDirection = existingTrade && existingTrade.direction === signal.direction;

          if (existingTrade && !isSameDirection) {
            return { item, tf, signal, existingTrade, type: "reversal" };
          } else if (!existingTrade) {
            return { item, tf, signal, existingTrade: null, type: "new" };
          }
        }
      } catch (error) {}
    }

    return { item, status: "no_signal" };
  });

  for (const result of scanResults) {
    if (!result) continue;

    if (result.type === "reversal") {
      const { item, tf, signal, existingTrade } = result;
      const warning = formatReversalWarning(item.symbol, existingTrade, signal, tf);
      const newSignalMsg = formatSignal(item.symbol, signal, tf);
      
      try {
        await sendTelegram(token, chatId, warning + newSignalMsg);
        console.log(`⚠️ Reversal on ${item.symbol} (${tf}): ${existingTrade.direction} -> ${signal.direction}`);
        
        openTrades = openTrades.filter(t => t.symbol !== item.symbol);
        openTrades.push({
          symbol: item.symbol,
          exchange: item.exchange,
          timeframe: tf,
          direction: signal.direction,
          entry: signal.entry,
          sl: signal.sl,
          tp: signal.tp,
          confirmationTime: signal.confirmationTime,
          status: "OPEN"
        });
        signalCount++;
      } catch (e) {}
    } else if (result.type === "new") {
      const { item, tf, signal } = result;
      try {
        signalCount++;
        const message = formatSignal(item.symbol, signal, tf);
        await sendTelegram(token, chatId, message);
        console.log(`[${signalCount}] ${item.symbol} (${item.exchange}) [${tf}] -> ${signal.direction}`);
        
        openTrades.push({
          symbol: item.symbol,
          exchange: item.exchange,
          timeframe: tf,
          direction: signal.direction,
          entry: signal.entry,
          sl: signal.sl,
          tp: signal.tp,
          confirmationTime: signal.confirmationTime,
          status: "OPEN"
        });
      } catch (e) {}
    }
  }

  saveTrades(openTrades);
  console.log(`Scan complete. Sent ${signalCount} new signals. Total open trades: ${openTrades.length}`);
  process.exit(0);
}

main().catch((error) => {
  console.error("Fatal error:", error.message);
  process.exit(1);
});
