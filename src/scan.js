const EXCHANGES = {
  okx: { name: "OKX", base: "https://www.okx.com" },
  bybit: { name: "Bybit", base: "https://api.bybit.com" },
  toobit: { name: "Toobit", base: "https://api.toobit.com" },
};

const CONFIG = {
  TIMEFRAME: "15m",
  INTERVAL_MS: 15 * 60 * 1000,
  FETCH_CANDLES: 130,
  MIN_CANDLES_REQUIRED: 30,

  TOP_SYMBOL_COUNT: 9999999999,
  MIN_QUOTE_VOLUME_24H: 5000,
  ANALYSIS_ORDER: ["okx", "bybit", "toobit"],

  API_MAX_RETRIES: 3,
  REQUEST_TIMEOUT_MS: 15000,
  MESSAGE_DELAY_MS: 1500,
  TOTAL_TIMEOUT_MS: 12 * 60 * 1000,

  DOJI_MAX_BODY_RATIO: 0.10,
  WICK_SYMMETRY_TOLERANCE: 0.15,

  SL_BUFFER_PERCENT: 0,
  SL_ATR_MULTIPLIER: 0.5,
  MIN_RR: 1.5,
  MAX_RR: 4,

  USE_CONFIRMATION_ATR_FILTERS: true,
  USE_SMA_FILTER: true,
  USE_RANGE_POSITION_FILTER: true,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

      if (!response.ok) {
        throw new Error(`${exchangeName} HTTP ${response.status}`);
      }

      const json = await response.json();
      validateJson(json);
      return json;
    } catch (error) {
      lastError = error;
      if (attempt < CONFIG.API_MAX_RETRIES - 1) {
        await sleep(500 * (attempt + 1));
      }
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

const okxFetch = createExchangeFetcher(
  EXCHANGES.okx.base,
  "OKX",
  (json) => {
    if (!json || json.code !== "0") throw new Error(`OKX: ${json?.msg || "unknown error"}`);
  }
);

const bybitFetch = createExchangeFetcher(
  EXCHANGES.bybit.base,
  "Bybit",
  (json) => {
    if (!json || json.retCode !== 0) throw new Error(`Bybit: ${json?.retMsg || "unknown error"}`);
  }
);

const toobitFetch = createExchangeFetcher(
  EXCHANGES.toobit.base,
  "Toobit",
  (json) => {
    if (json == null) throw new Error("Toobit: empty response");
  }
);

async function fetchOKXSymbols() {
  const json = await okxFetch("/api/v5/public/instruments", { instType: "SWAP" });
  return (json.data || [])
    .filter(
      (item) =>
        item.state === "live" &&
        item.settleCcy === "USDT" &&
        item.ctType === "linear" &&
        item.instId.endsWith("-USDT-SWAP")
    )
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
      const volume = parseFloat(item.volCcy24h || "0");
      return { symbol: `${base}USDT`, volume: Number.isFinite(volume) ? volume : 0 };
    });
}

async function fetchBybitSymbols() {
  const all = [];
  let cursor = "";

  for (let page = 0; page < 10; page++) {
    const params = { category: "linear", limit: "1000" };
    if (cursor) params.cursor = cursor;

    const json = await bybitFetch("/v5/market/instruments-info", params);
    const rows = json.result?.list || [];
    all.push(...rows);

    cursor = json.result?.nextPageCursor || "";
    if (!cursor || rows.length === 0) break;
  }

  return all
    .filter(
      (item) =>
        item.status === "Trading" &&
        item.quoteCoin === "USDT" &&
        item.contractType === "LinearPerpetual"
    )
    .map((item) => ({
      symbol: item.symbol,
      base: item.baseCoin,
      exchange: "bybit",
      raw: item.symbol,
    }));
}

async function fetchBybitTickers() {
  const json = await bybitFetch("/v5/market/tickers", { category: "linear" });
  return (json.result?.list || [])
    .filter((item) => item.symbol.endsWith("USDT"))
    .map((item) => {
      const volume = parseFloat(item.turnover24h || "0");
      return { symbol: item.symbol, volume: Number.isFinite(volume) ? volume : 0 };
    });
}

async function fetchToobitSymbols() {
  const json = await toobitFetch("/api/v1/exchangeInfo");
  return (json.contracts || [])
    .filter(
      (item) =>
        item.status === "TRADING" &&
        item.inverse === false &&
        String(item.symbol || "").endsWith("-SWAP-USDT")
    )
    .map((item) => {
      const raw = item.symbol;
      const base = item.underlying || raw.split("-")[0];
      return { symbol: `${base}USDT`, base, exchange: "toobit", raw };
    });
}

async function fetchToobitTickers() {
  const json = await toobitFetch("/quote/v1/contract/ticker/24hr");
  const rows = Array.isArray(json) ? json : json?.data || json?.result || [];

  return rows
    .map((item) => {
      const raw = String(item.s || item.symbol || "");
      const normalized = raw.replace("-SWAP-USDT", "USDT");
      const volume = parseFloat(item.qv || item.quoteVolume || item.volume || "0");
      return { symbol: normalized, volume: Number.isFinite(volume) ? volume : 0 };
    })
    .filter((item) => item.symbol && item.volume > 0);
}

async function getTopSymbols() {
  const allCandidates = [];
  const exchangeSources = [
    { name: "okx", fetchSymbols: fetchOKXSymbols, fetchTickers: fetchOKXTickers },
    { name: "bybit", fetchSymbols: fetchBybitSymbols, fetchTickers: fetchBybitTickers },
    { name: "toobit", fetchSymbols: fetchToobitSymbols, fetchTickers: fetchToobitTickers },
  ];

  const orderedSources = CONFIG.ANALYSIS_ORDER
    .map((name) => exchangeSources.find((ex) => ex.name === name))
    .filter(Boolean);

  for (const exchange of orderedSources) {
    try {
      console.log(`  Trying ${exchange.name}...`);
      const [symbols, tickers] = await Promise.all([
        exchange.fetchSymbols(),
        exchange.fetchTickers(),
      ]);

      console.log(`  ${exchange.name}: ${symbols.length} symbols, ${tickers.length} tickers`);

      if (!symbols || symbols.length === 0) {
        console.log(`  ${exchange.name}: empty symbols list`);
        continue;
      }

      const volumeMap = new Map(tickers.map((t) => [t.symbol, t.volume]));
      const ranked = symbols
        .map((s) => ({ ...s, volume: volumeMap.get(s.symbol) || 0 }))
        .filter((s) => s.volume >= CONFIG.MIN_QUOTE_VOLUME_24H)
        .sort((a, b) => b.volume - a.volume);

      console.log(`  ${exchange.name}: ${ranked.length} symbols after volume filter`);
      allCandidates.push(...ranked);
    } catch (error) {
      console.log(`  ${exchange.name}: ${error.message}`);
    }
  }

  if (allCandidates.length === 0) {
    throw new Error("No symbols from any exchange");
  }

  const uniqueMap = new Map();
  for (const item of allCandidates) {
    const existing = uniqueMap.get(item.symbol);
    if (!existing || item.volume > existing.volume) {
      uniqueMap.set(item.symbol, item);
    }
  }

  return [...uniqueMap.values()]
    .sort((a, b) => b.volume - a.volume)
    .slice(0, CONFIG.TOP_SYMBOL_COUNT);
}

function isValidCandle(c) {
  return (
    Number.isFinite(c.time) &&
    Number.isFinite(c.open) &&
    Number.isFinite(c.high) &&
    Number.isFinite(c.low) &&
    Number.isFinite(c.close) &&
    c.high >= c.low &&
    c.high >= Math.max(c.open, c.close) &&
    c.low <= Math.min(c.open, c.close) &&
    c.low > 0
  );
}

async function getCandles(symbol, exchange) {
  const base = symbol.replace(/USDT$/, "");
  const exchangesToTry = [exchange, ...CONFIG.ANALYSIS_ORDER.filter((e) => e !== exchange)];
  let lastError = null;

  for (const currentExchange of exchangesToTry) {
    try {
      let candles = [];

      if (currentExchange === "okx") {
        const json = await okxFetch("/api/v5/market/candles", {
          instId: `${base}-USDT-SWAP`,
          bar: CONFIG.TIMEFRAME,
          limit: String(CONFIG.FETCH_CANDLES),
        });
        if (!Array.isArray(json.data)) throw new Error("Invalid OKX candle response");
        candles = json.data.map((row) => ({
          time: Number(row[0]),
          open: Number(row[1]),
          high: Number(row[2]),
          low: Number(row[3]),
          close: Number(row[4]),
          closed: row[8] === "1",
        }));
      } else if (currentExchange === "bybit") {
        const json = await bybitFetch("/v5/market/kline", {
          category: "linear",
          symbol,
          interval: "15",
          limit: String(CONFIG.FETCH_CANDLES),
        });
        if (!Array.isArray(json.result?.list)) throw new Error("Invalid Bybit candle response");
        candles = json.result.list.map((row) => ({
          time: Number(row[0]),
          open: Number(row[1]),
          high: Number(row[2]),
          low: Number(row[3]),
          close: Number(row[4]),
          closed: Number(row[0]) + CONFIG.INTERVAL_MS <= Date.now(),
        }));
      } else if (currentExchange === "toobit") {
        const json = await toobitFetch("/quote/v1/klines", {
          symbol: `${base}-SWAP-USDT`,
          interval: CONFIG.TIMEFRAME,
          limit: String(CONFIG.FETCH_CANDLES),
        });
        const rows = Array.isArray(json) ? json : json?.data || json?.result || [];
        if (!Array.isArray(rows)) throw new Error("Invalid Toobit candle response");

        candles = rows.map((row) => {
          if (Array.isArray(row)) {
            const time = Number(row[0]);
            const closeTime = Number(row[6]);
            return {
              time,
              open: Number(row[1]),
              high: Number(row[2]),
              low: Number(row[3]),
              close: Number(row[4]),
              closed: (Number.isFinite(closeTime) && closeTime > time ? closeTime : time + CONFIG.INTERVAL_MS) <= Date.now(),
            };
          }
          const time = Number(row.time ?? row.t ?? row.openTime);
          const closeTime = Number(row.closeTime ?? row.T ?? (time + CONFIG.INTERVAL_MS));
          return {
            time,
            open: Number(row.open ?? row.o),
            high: Number(row.high ?? row.h),
            low: Number(row.low ?? row.l),
            close: Number(row.close ?? row.c),
            closed: closeTime <= Date.now(),
          };
        });
      }

      candles.sort((a, b) => a.time - b.time);
      const closed = candles.filter((c) => c.closed && isValidCandle(c));

      const unique = [];
      const seen = new Set();
      for (const c of closed) {
        if (!seen.has(c.time)) {
          seen.add(c.time);
          unique.push(c);
        }
      }

      if (unique.length >= CONFIG.MIN_CANDLES_REQUIRED) {
        return unique.slice(-CONFIG.FETCH_CANDLES);
      }
      throw new Error(`Only ${unique.length} valid candles from ${currentExchange}`);
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("No candles available");
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

  if (upperWick === 0 || lowerWick === 0) return null;

  if (Math.abs(upperWick - lowerWick) / wickSum <= CONFIG.WICK_SYMMETRY_TOLERANCE) return "standard";

  return null;
}

function calculateATR(candles, period = 14) {
  if (!candles || candles.length < period + 1) return 0;

  const trs = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    if (i === 0) {
      trs.push(c.high - c.low);
    } else {
      const prevClose = candles[i - 1].close;
      trs.push(Math.max(c.high - c.low, Math.abs(c.high - prevClose), Math.abs(c.low - prevClose)));
    }
  }
  return trs.slice(-period).reduce((sum, val) => sum + val, 0) / period;
}

function applyTechnicalFilters(candles, dojiIndex, direction, doji, confirmation, atrValue, sma50) {
  const confirmationRange = confirmation.high - confirmation.low;

  if (CONFIG.USE_CONFIRMATION_ATR_FILTERS) {
    if (confirmationRange < atrValue * 0.5) return false;

    const confirmationBody = Math.abs(confirmation.close - confirmation.open);
    if (confirmationBody < atrValue * 0.3) return false;

    const breakoutDistance = direction === "LONG"
      ? confirmation.close - doji.high
      : doji.low - confirmation.close;

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
  const sma50 = candles.slice(-sma50Period).reduce((sum, c) => sum + c.close, 0) / sma50Period;

  const passedFilters = applyTechnicalFilters(
    candles, dojiIndex, direction, doji, confirmation, atrValue, sma50
  );
  if (!passedFilters) return null;

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
    tradeStatus: "OPEN",
    dojiTime: doji.time,
    confirmationTime: confirmation.time,
    currentPrice: confirmation.close,
  };
}

function fmtPrice(value) {
  return Number.isFinite(value)
    ? value.toLocaleString("en-US", { maximumFractionDigits: 10, useGrouping: false })
    : "N/A";
}

function fmtTime(timestamp) {
  return new Date(timestamp).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function escapeHtml(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function formatSignal(symbol, signal) {
  const isLong = signal.direction === "LONG";
  const directionIcon = isLong ? "🟢" : "🔴";
  const directionText = isLong ? "LONG" : "SHORT";

  const statusText = `معامله باز است (R فعلی: 0.00)`;
  const statusIcon = "⏳";

  return (
    "📊 <b>تحلیل Doji</b>\n\n" +
    `${directionIcon} <b>${escapeHtml(symbol)}</b>\n` +
    `⏱ تایم‌فریم: ${CONFIG.TIMEFRAME}\n` +
    `📌 وضعیت: ${directionIcon} <b>${directionText}</b>\n\n` +
    `💰 قیمت فعلی: <b>${fmtPrice(signal.currentPrice)}</b>\n\n` +
    "🎯 <b>Doji Setup فعال:</b>\n" +
    `🕯 نوع دوجی: <b>Standard</b>\n` +
    `🎯 ورود: <b>${fmtPrice(signal.entry)}</b>\n` +
    `🛑 حد ضرر: <b>${fmtPrice(signal.sl)}</b>\n` +
    `📏 ریسک: <b>${fmtPrice(signal.risk)}</b>\n\n` +
    `🥇 TP: <b>${fmtPrice(signal.tp)}</b>\n` +
    `⚖️ R:R: <b>1:${signal.rr.toFixed(2)}</b>\n\n` +
    `${statusIcon} <b>نتیجه: ${statusText}</b>\n` +
    `🕯 زمان تأیید: ${fmtTime(signal.confirmationTime)}\n\n` +
    "━━━━━━━━━━━━━━━━━━━━━━\n" +
    "💡 مدیریت ریسک و سرمایه:\n" +
    "🔸 با تاچ شدن TP، حد ضرر را به Breakeven منتقل کنید.\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
    "⚠️ این خروجی تحلیل الگوریتمی است و تضمین سود نیست."
  );
}

async function sendTelegram(token, chatId, message) {
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CONFIG.REQUEST_TIMEOUT_MS);

  let response;
  let data;

  try {
    response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text: message,
        parse_mode: "HTML",
        disable_web_page_preview: true,
      }),
      signal: controller.signal,
    });
    data = await response.json();
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok || !data?.ok) {
    throw new Error(data?.description || `Telegram error HTTP ${response.status}`);
  }
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

  let symbols = [];
  try {
    symbols = await getTopSymbols();
  } catch (error) {
    console.error("Failed to fetch symbols:", error.message);
    process.exit(1);
  }

  console.log(`Fetched ${symbols.length} symbols to analyze.`);
  if (symbols.length > 0) {
    console.log(`Top 5: ${symbols.slice(0, 5).map((i) => i.symbol).join(", ")}`);
  }

  let signalCount = 0;

  for (let i = 0; i < symbols.length; i++) {
    if (Date.now() - startTime > CONFIG.TOTAL_TIMEOUT_MS) {
      console.log(`Timeout reached at symbol ${i + 1}/${symbols.length}`);
      break;
    }

    const item = symbols[i];

    try {
      const candles = await getCandles(item.symbol, item.exchange);
      const signal = analyzeSymbol(candles);

      if (signal) {
        signalCount++;
        const message = formatSignal(item.symbol, signal);
        await sendTelegram(token, chatId, message);
        console.log(`[${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) -> ${signal.direction} (${signal.tradeStatus})`);
      } else {
        console.log(`[${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) -> no confirmed Doji`);
      }
    } catch (error) {
      console.log(`[${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) -> ${error.message}`);
    }

    await sleep(CONFIG.MESSAGE_DELAY_MS);
  }

  console.log(`Scan complete. Found and sent ${signalCount} signal(s).`);
  process.exit(0);
}

main().catch((error) => {
  console.error("Fatal error:", error.message);
  process.exit(1);
});
