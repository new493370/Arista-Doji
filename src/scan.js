import fs from 'fs';
import path from 'path';

const EXCHANGES = {
  binance: { name: "Binance", futuresBase: "https://fapi1.binance.com", spotBase: "https://data-api.binance.vision" },
  okx: { name: "OKX", base: "https://www.okx.com" },
  mexc: { name: "MEXC", base: "https://contract.mexc.com" },
};

const CONFIG = {
  TIMEFRAME: "15m",
  INTERVAL_MS: 15 * 60 * 1000,
  FETCH_CANDLES: 130,
  MIN_CANDLES_REQUIRED: 30,
  TRADES_FILE: path.resolve('src', 'trades.json'),

  TOP_SYMBOL_COUNT: 9999999999,
  MIN_QUOTE_VOLUME_24H: 100000,
  ANALYSIS_ORDER: ["binance", "okx", "mexc"],

  API_MAX_RETRIES: 3,
  REQUEST_TIMEOUT_MS: 15000,
  MESSAGE_DELAY_MS: 200,
  TOTAL_TIMEOUT_MS: 12 * 60 * 1000,

  CONCURRENCY: 25,
  RATE_LIMIT_BINANCE: 15,
  RATE_LIMIT_OKX: 8,
  RATE_LIMIT_MEXC: 8,
  RATE_LIMIT_WINDOW_MS: 1000,

  DOJI_MAX_BODY_RATIO: 0.10,
  WICK_SYMMETRY_TOLERANCE: 0.15,
  MIN_WICK_RATIO: 0.05,

  SL_BUFFER_PERCENT: 0,
  SL_ATR_MULTIPLIER: 0.8,
  MIN_RR: 1.0,
  MAX_RR: 5,

  CONFIRMATION_MIN_RANGE_ATR: 0.25,
  CONFIRMATION_MIN_BODY_ATR: 0.10,
  CONFIRMATION_MAX_BREAKOUT_ATR: 3.0,
  CONFIRMATION_MIN_VOLUME_RATIO: 0.9,
  RANGE_LOW_TOLERANCE: 0.05,
  RANGE_HIGH_TOLERANCE: 0.10,

  USE_CONFIRMATION_ATR_FILTERS: true,
  USE_SMA_FILTER: true,
  USE_RANGE_POSITION_FILTER: true,
  USE_VOLUME_FILTER: true,

  SYMBOL_CACHE_SECONDS: 3600,
  TICKERS_CACHE_SECONDS: 300,

  EXCLUDED_BASE_ASSETS: [
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI",
    "USDE", "USDS", "EUR", "TRY", "BRL", "GBP",
    "AUD", "JPY", "RUB", "UAH", "PLN", "ARS",
    "MXN", "ZAR", "USD1", "PYUSD", "USDP", "GUSD"
  ],
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const rateLimits = {
  binance: { max: CONFIG.RATE_LIMIT_BINANCE, window: CONFIG.RATE_LIMIT_WINDOW_MS, timestamps: [] },
  okx: { max: CONFIG.RATE_LIMIT_OKX, window: CONFIG.RATE_LIMIT_WINDOW_MS, timestamps: [] },
  mexc: { max: CONFIG.RATE_LIMIT_MEXC, window: CONFIG.RATE_LIMIT_WINDOW_MS, timestamps: [] },
};

async function withRateLimit(exchange, fn) {
  const rl = rateLimits[exchange];
  if (!rl) return fn();

  while (true) {
    const now = Date.now();
    rl.timestamps = rl.timestamps.filter((t) => now - t < rl.window);
    if (rl.timestamps.length < rl.max) {
      rl.timestamps.push(now);
      return fn();
    }
    await sleep(20);
  }
}

async function runWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;
  let stopped = false;

  async function runner() {
    while (!stopped) {
      const currentIndex = nextIndex++;
      if (currentIndex >= items.length) return;
      try {
        results[currentIndex] = await worker(items[currentIndex], currentIndex);
      } catch (e) {
        results[currentIndex] = null;
      }
    }
  }

  const runners = [];
  const runnerCount = Math.min(concurrency, items.length || 1);
  for (let i = 0; i < runnerCount; i++) {
    runners.push(runner());
  }
  await Promise.all(runners);

  return results;
}

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

      if (response.status === 418) {
        await sleep(Math.min(5000 * (attempt + 1), 30000));
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

const binanceFuturesFetch = createExchangeFetcher(EXCHANGES.binance.futuresBase, "Binance", (json) => {
  if (json == null || (Array.isArray(json) && json.length === 0)) throw new Error(`Binance: invalid response`);
});

const binanceSpotFetch = createExchangeFetcher(EXCHANGES.binance.spotBase, "BinanceSpot", (json) => {
  if (json == null || (Array.isArray(json) && json.length === 0)) throw new Error(`BinanceSpot: invalid response`);
});

const okxFetch = createExchangeFetcher(EXCHANGES.okx.base, "OKX", (json) => {
  if (!json || json.code !== "0") throw new Error(`OKX: ${json?.msg || "unknown error"}`);
});

const mexcFetch = createExchangeFetcher(EXCHANGES.mexc.base, "MEXC", (json) => {
  if (json == null || (json.success !== undefined && !json.success)) throw new Error(`MEXC: invalid response`);
});

function isExcludedBase(base) {
  if (!base) return true;
  const upper = String(base).toUpperCase();
  if (CONFIG.EXCLUDED_BASE_ASSETS.includes(upper)) return true;
  if (/^X[A-Z]{4,}$/.test(upper)) return true;
  if (upper.includes("TEST")) return true;
  return false;
}

let symbolCache = { timestamp: 0, data: [] };
let tickersCache = { timestamp: 0, data: [] };

async function fetchBinanceSpotSymbols() {
  const json = await binanceSpotFetch("/api/v3/exchangeInfo");
  const result = [];
  for (const item of json.symbols || []) {
    if (item.status !== "TRADING") continue;
    if (item.quoteAsset !== "USDT") continue;
    const base = String(item.baseAsset || "").toUpperCase();
    if (isExcludedBase(base)) continue;
    result.push({ symbol: base + "USDT", base, exchange: "binance", raw: item.symbol });
  }
  return result;
}

async function fetchBinanceSpotTickers() {
  const json = await binanceSpotFetch("/api/v3/ticker/24hr");
  if (!Array.isArray(json)) return [];
  const result = [];
  for (const item of json) {
    const raw = String(item.symbol || "").toUpperCase();
    if (!raw.endsWith("USDT")) continue;
    const base = raw.replace(/USDT$/, "");
    if (isExcludedBase(base)) continue;
    const last = parseFloat(item.lastPrice || "0");
    const quoteVolume = parseFloat(item.quoteVolume || "0");
    if (!Number.isFinite(last) || last <= 0) continue;
    result.push({ symbol: base + "USDT", volume: Number.isFinite(quoteVolume) ? quoteVolume : 0 });
  }
  return result;
}

async function fetchOKXSymbols() {
  const json = await okxFetch("/api/v5/public/instruments", { instType: "SWAP" });
  const result = [];
  for (const item of json.data || []) {
    if (item.state !== "live") continue;
    if (item.settleCcy !== "USDT") continue;
    if (item.ctType !== "linear") continue;
    if (!String(item.instId || "").endsWith("-USDT-SWAP")) continue;
    const base = String(item.ctValCcy || item.baseCcy || item.instId.split("-")[0] || "").toUpperCase();
    if (!base) continue;
    if (isExcludedBase(base)) continue;
    result.push({ symbol: base + "USDT", base, exchange: "okx", raw: item.instId });
  }
  return result;
}

async function fetchOKXTickers() {
  const json = await okxFetch("/api/v5/market/tickers", { instType: "SWAP" });
  const result = [];
  for (const item of json.data || []) {
    if (!String(item.instId || "").endsWith("-USDT-SWAP")) continue;
    const base = item.instId.split("-")[0];
    if (!base) continue;
    if (isExcludedBase(base)) continue;
    const last = parseFloat(item.last || "0");
    const volCcy24h = parseFloat(item.volCcy24h || "0");
    const vol24h = parseFloat(item.vol24h || "0");
    if (!Number.isFinite(last) || last <= 0) continue;
    const quoteVolume =
      Number.isFinite(volCcy24h) && volCcy24h > 0
        ? volCcy24h
        : Number.isFinite(vol24h)
          ? vol24h * last
          : 0;
    result.push({ symbol: base + "USDT", volume: quoteVolume });
  }
  return result;
}

async function fetchMEXCSymbols() {
  const json = await mexcFetch("/api/v1/contract/detail");
  const result = [];
  for (const item of json.data || []) {
    if (item.state !== 0) continue;
    if (item.quoteCoin !== "USDT") continue;
    if (item.settleCoin !== "USDT") continue;
    const base = String(item.baseCoin || "").toUpperCase();
    if (!base) continue;
    if (isExcludedBase(base)) continue;
    result.push({ symbol: base + "USDT", base, exchange: "mexc", raw: item.symbol });
  }
  return result;
}

async function fetchMEXCTickers() {
  const json = await mexcFetch("/api/v1/contract/ticker");
  const result = [];
  for (const item of json.data || []) {
    const raw = String(item.symbol || "").toUpperCase();
    if (!raw.endsWith("USDT")) continue;
    if (!raw.includes("_USDT")) continue;
    const base = raw.replace(/_USDT$/, "").replace(/USDT$/, "");
    if (!base) continue;
    if (isExcludedBase(base)) continue;
    const last = parseFloat(item.lastPrice || "0");
    const quoteVolume = parseFloat(item.amount24 || item.volume24 || "0");
    if (!Number.isFinite(last) || last <= 0) continue;
    result.push({ symbol: base + "USDT", volume: Number.isFinite(quoteVolume) ? quoteVolume : 0 });
  }
  return result;
}

async function getExchangeSymbols() {
  const now = Date.now();
  if (symbolCache.data.length > 0 && (now - symbolCache.timestamp) < CONFIG.SYMBOL_CACHE_SECONDS * 1000) {
    return symbolCache.data;
  }

  const allSymbols = new Map();
  const sources = [
    { name: "binance", fetcher: fetchBinanceSpotSymbols },
    { name: "okx", fetcher: fetchOKXSymbols },
    { name: "mexc", fetcher: fetchMEXCSymbols },
  ];

  await Promise.allSettled(
    sources.map(async (src) => {
      try {
        const symbols = await src.fetcher();
        for (const item of symbols) {
          if (!allSymbols.has(item.symbol)) {
            allSymbols.set(item.symbol, item);
          }
        }
      } catch (e) {
        console.error(`[${src.name}] symbols error:`, e.message || e);
      }
    })
  );

  const result = Array.from(allSymbols.values());
  result.sort((a, b) => a.symbol.localeCompare(b.symbol));
  symbolCache = { timestamp: now, data: result };
  return result;
}

async function getTickers() {
  const now = Date.now();
  if (tickersCache.data.length > 0 && (now - tickersCache.timestamp) < CONFIG.TICKERS_CACHE_SECONDS * 1000) {
    return tickersCache.data;
  }

  const validSymbols = await getExchangeSymbols();
  const validSet = new Set(validSymbols.map((s) => s.symbol));

  const bySymbol = new Map();
  const sources = [
    { name: "binance", fetcher: fetchBinanceSpotTickers },
    { name: "okx", fetcher: fetchOKXTickers },
    { name: "mexc", fetcher: fetchMEXCTickers },
  ];

  await Promise.allSettled(
    sources.map(async (src) => {
      try {
        const tickers = await src.fetcher();
        for (const t of tickers) {
          if (!validSet.has(t.symbol)) continue;
          const existing = bySymbol.get(t.symbol);
          if (!existing || t.volume > existing.volume) {
            bySymbol.set(t.symbol, { symbol: t.symbol, volume: t.volume, source: src.name });
          }
        }
      } catch (e) {
        console.error(`[${src.name}] tickers error:`, e.message || e);
      }
    })
  );

  const result = Array.from(bySymbol.values());
  result.sort((a, b) => b.volume - a.volume);
  tickersCache = { timestamp: now, data: result };
  return result;
}

async function getTopSymbols() {
  const symbols = await getExchangeSymbols();
  const tickers = await getTickers();

  const symbolMap = new Map(symbols.map((s) => [s.symbol, s]));
  const ranked = [];

  for (const ticker of tickers) {
    if (!Number.isFinite(ticker.volume) || ticker.volume < CONFIG.MIN_QUOTE_VOLUME_24H) continue;
    const symbolInfo = symbolMap.get(ticker.symbol);
    if (!symbolInfo) continue;
    ranked.push({ ...symbolInfo, volume: ticker.volume, exchange: symbolInfo.exchange });
  }

  ranked.sort((a, b) => b.volume - a.volume);
  return ranked.slice(0, CONFIG.TOP_SYMBOL_COUNT);
}

function isValidCandle(c) {
  return Number.isFinite(c.time) && Number.isFinite(c.open) && Number.isFinite(c.high) && Number.isFinite(c.low) && Number.isFinite(c.close) && c.high >= c.low && c.high >= Math.max(c.open, c.close) && c.low <= Math.min(c.open, c.close) && c.low > 0;
}

async function getCandlesFromBinance(symbol) {
  const json = await binanceFuturesFetch("/fapi/v1/klines", { symbol, interval: CONFIG.TIMEFRAME, limit: String(CONFIG.FETCH_CANDLES) });
  if (!Array.isArray(json)) throw new Error("Invalid Binance klines");
  return json.map((row) => ({
    time: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]) || 0,
    closed: Number(row[6]) <= Date.now(),
  }));
}

async function getCandlesFromOKX(symbol) {
  const base = symbol.replace(/USDT$/, "");
  const json = await okxFetch("/api/v5/market/candles", { instId: `${base}-USDT-SWAP`, bar: CONFIG.TIMEFRAME, limit: String(CONFIG.FETCH_CANDLES) });
  if (!Array.isArray(json.data)) throw new Error("Invalid OKX klines");
  return json.data.map((row) => ({
    time: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]) || 0,
    closed: (Number(row[0]) + CONFIG.INTERVAL_MS) <= Date.now(),
  }));
}

async function getCandlesFromMEXC(symbol) {
  const base = symbol.replace(/USDT$/, "");
  const mexcSymbol = base + "_USDT";
  const json = await mexcFetch("/api/v1/contract/kline/" + mexcSymbol, { interval: "Min15", limit: String(CONFIG.FETCH_CANDLES) });
  if (!json.data || !Array.isArray(json.data.time)) throw new Error("Invalid MEXC klines");
  const { time, open, high, low, close, vol } = json.data;
  return time.map((t, i) => ({
    time: Number(t) * 1000,
    open: Number(open[i]),
    high: Number(high[i]),
    low: Number(low[i]),
    close: Number(close[i]),
    volume: Number(vol?.[i]) || 0,
    closed: (Number(t) * 1000) + CONFIG.INTERVAL_MS <= Date.now(),
  }));
}

async function getCandles(symbol, exchange) {
  try {
    let candles = [];

    if (exchange === "binance") {
      candles = await withRateLimit("binance", () => getCandlesFromBinance(symbol));
    } else if (exchange === "okx") {
      candles = await withRateLimit("okx", () => getCandlesFromOKX(symbol));
    } else if (exchange === "mexc") {
      candles = await withRateLimit("mexc", () => getCandlesFromMEXC(symbol));
    } else {
      return null;
    }

    if (!Array.isArray(candles) || candles.length === 0) return null;

    candles.sort((a, b) => a.time - b.time);
    const closed = candles.filter((c) => c.closed && isValidCandle(c));
    const unique = [];
    const seen = new Set();
    for (const c of closed) {
      if (!seen.has(c.time)) { seen.add(c.time); unique.push(c); }
    }

    if (unique.length >= CONFIG.MIN_CANDLES_REQUIRED) {
      return unique.slice(-CONFIG.FETCH_CANDLES);
    }
    return null;
  } catch (error) {
    return null;
  }
}

function getDojiType(candle) {
  const range = candle.high - candle.low;
  if (!Number.isFinite(range) || range <= 0) return null;
  const body = Math.abs(candle.close - candle.open);
  if (body / range > CONFIG.DOJI_MAX_BODY_RATIO) return null;
  const upperWick = candle.high - Math.max(candle.open, candle.close);
  const lowerWick = Math.min(candle.open, candle.close) - candle.low;
  if (upperWick < 0 || lowerWick < 0) return null;
  const wickSum = upperWick + lowerWick;
  if (wickSum <= 0) return null;
  if (wickSum < range * CONFIG.MIN_WICK_RATIO) return null;
  if (upperWick === 0 && lowerWick === 0) return null;
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

  if (CONFIG.USE_CONFIRMATION_ATR_FILTERS) {
    if (confirmationRange < atrValue * CONFIG.CONFIRMATION_MIN_RANGE_ATR) return false;
    const confirmationBody = Math.abs(confirmation.close - confirmation.open);
    if (confirmationBody < atrValue * CONFIG.CONFIRMATION_MIN_BODY_ATR) return false;
    const breakoutDistance = direction === "LONG" ? confirmation.close - doji.high : doji.low - confirmation.close;
    if (breakoutDistance > atrValue * CONFIG.CONFIRMATION_MAX_BREAKOUT_ATR) return false;
  }

  if (CONFIG.USE_SMA_FILTER) {
    if (direction === "LONG" && doji.close < sma50) return false;
    if (direction === "SHORT" && doji.close > sma50) return false;
  }

  if (CONFIG.USE_VOLUME_FILTER) {
    const volEnd = dojiIndex;
    const volStart = Math.max(0, volEnd - 20);
    const histVols = candles.slice(volStart, volEnd).map((c) => c.volume || 0).filter((v) => v > 0);
    if (histVols.length >= 5) {
      const avgVol = histVols.reduce((s, v) => s + v, 0) / histVols.length;
      if (avgVol > 0 && (confirmation.volume || 0) < avgVol * CONFIG.CONFIRMATION_MIN_VOLUME_RATIO) return false;
    }
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
    const mid = (recentHigh + recentLow) / 2;
    const distanceFromHigh = (recentHigh - doji.high) / range100;
    const distanceFromLow = (doji.low - recentLow) / range100;

    if (direction === "LONG") {
      if (distanceFromHigh < CONFIG.RANGE_HIGH_TOLERANCE) return false;
      if (distanceFromLow > 0.35) return false;
      if (dojiMid > mid) return false;
    } else {
      if (distanceFromLow < CONFIG.RANGE_HIGH_TOLERANCE) return false;
      if (distanceFromHigh > 0.35) return false;
      if (dojiMid < mid) return false;
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

  const sma50Period = Math.min(50, dojiIndex + 1);
  if (sma50Period < 5) return null;
  const sma50Slice = candles.slice(dojiIndex + 1 - sma50Period, dojiIndex + 1);
  const sma50 = sma50Slice.reduce((s, c) => s + c.close, 0) / sma50Slice.length;
  if (!Number.isFinite(sma50) || sma50 <= 0) return null;

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

  const validLevels = structuralLevels.filter((level) => {
    const rr = direction === "LONG" ? (level - entry) / risk : (entry - level) / risk;
    return rr >= CONFIG.MIN_RR && rr <= CONFIG.MAX_RR;
  });

  if (validLevels.length === 0) return null;

  validLevels.sort((a, b) => direction === "LONG" ? a - b : b - a);
  const tp = validLevels[0];

  const reward = direction === "LONG" ? tp - entry : entry - tp;
  if (!Number.isFinite(reward) || reward <= 0) return null;

  const finalRR = reward / risk;
  if (finalRR < CONFIG.MIN_RR || finalRR > CONFIG.MAX_RR) return null;

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

function formatSignal(symbol, signal) {
  const isLong = signal.direction === "LONG";
  const dIcon = isLong ? "🟢" : "🔴";
  const dText = isLong ? "LONG" : "SHORT";

  return (
    "📊 <b>تحلیل Doji</b>\n\n" +
    `${dIcon} <b>${escapeHtml(symbol)}</b>\n` +
    `⏱ تایم‌فریم: ${CONFIG.TIMEFRAME}\n` +
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
    `⏱ تایم‌فریم: ${CONFIG.TIMEFRAME}\n` +
    `📌 جهت: ${dIcon} <b>${dText}</b>\n\n` +
    `${reasonText}\n` +
    `💰 سود/ضرر خالص (بدون اهرم): <b>${profitColor} ${profitSign}${profitPct.toFixed(2)}%</b>\n\n` +
    `🎯 ورود: <b>${fmtPrice(trade.entry)}</b>\n` +
    `🏁 خروج: <b>${fmtPrice(hitPrice)}</b>\n\n` +
    "💡 مدیریت سرمایه را رعایت کنید. منتظر سیگنال بعدی باشید."
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

async function checkOpenTrades(openTrades, token, chatId) {
  if (openTrades.length === 0) return [];

  console.log(`Checking ${openTrades.length} open trades...`);
  const remainingTrades = [];

  for (const trade of openTrades) {
    try {
      const candles = await getCandles(trade.symbol, trade.exchange);

      if (!Array.isArray(candles) || candles.length === 0) {
        remainingTrades.push(trade);
        continue;
      }

      const sentTime = Number(trade.confirmationTime);
      const signalCandles = candles.filter(c => c.time > sentTime);

      if (signalCandles.length === 0) {
        remainingTrades.push(trade);
        continue;
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
        const message = formatTradeClosed(trade);
        await sendTelegram(token, chatId, message);
        console.log(`Trade closed: ${trade.symbol} -> ${closedStatus}`);
      } else {
        remainingTrades.push(trade);
      }
    } catch (error) {
      console.error(`Failed to check ${trade.symbol}: ${error.message}`);
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

  const openTradeSymbols = new Set(openTrades.map(t => t.symbol));

  const tasks = symbols.filter((item) => !openTradeSymbols.has(item.symbol));

  console.log(`Analyzing ${tasks.length} symbols (concurrency: ${CONFIG.CONCURRENCY})...`);

  const results = await runWithConcurrency(tasks, CONFIG.CONCURRENCY, async (item) => {
    if (Date.now() - startTime > CONFIG.TOTAL_TIMEOUT_MS) return null;

    try {
      const candles = await getCandles(item.symbol, item.exchange);
      if (!candles) return null;
      const signal = analyzeSymbol(candles);
      if (!signal) return null;
      return { item, signal };
    } catch (error) {
      return null;
    }
  });

  console.log(`Analysis done in ${((Date.now() - startTime) / 1000).toFixed(1)}s.`);

  const signals = results.filter((r) => r && r.item && r.signal);

  let signalCount = 0;

  for (const { item, signal } of signals) {
    try {
      const message = formatSignal(item.symbol, signal);
      await sendTelegram(token, chatId, message);
      console.log(`${item.symbol} (${item.exchange}) -> ${signal.direction}`);

      openTrades.push({
        symbol: item.symbol,
        exchange: item.exchange,
        direction: signal.direction,
        entry: signal.entry,
        sl: signal.sl,
        tp: signal.tp,
        confirmationTime: signal.confirmationTime,
        status: "OPEN"
      });

      signalCount++;
      await sleep(CONFIG.MESSAGE_DELAY_MS);
    } catch (error) {
      console.error(`Telegram error for ${item.symbol}:`, error.message);
    }
  }

  saveTrades(openTrades);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Scan complete in ${elapsed}s. Sent ${signalCount} new signals. Total open trades: ${openTrades.length}`);
  process.exit(0);
}

main().catch((error) => {
  console.error("Fatal error:", error.message);
  process.exit(1);
});
