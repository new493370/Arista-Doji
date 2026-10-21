const EXCHANGES = {
  okx: {
    name: "OKX",
    base: "https://www.okx.com",
  },
  bybit: {
    name: "Bybit",
    base: "https://api.bybit.com",
  },
  toobit: {
    name: "Toobit",
    base: "https://api.toobit.com",
  },
};

const CONFIG = {
  TIMEFRAME: "15m",
  INTERVAL_MS: 15 * 60 * 1000,

  HISTORY_CANDLES: 5,
  FETCH_CANDLES: 130,

  DOJI_MAX_BODY_RATIO: 0.10,
  WICK_SYMMETRY_TOLERANCE: 0.10,
  SL_BUFFER_PERCENT: 0,

  TOP_SYMBOL_COUNT: 20,
  MIN_24H_VOLUME: 10000,

  ANALYSIS_ORDER: ["toobit", "okx", "bybit"],

  API_MAX_RETRIES: 3,
  API_RETRY_DELAY_MS: 1000,

  MESSAGE_DELAY_MS: 1500,
  TOTAL_TIMEOUT_MS: 12 * 60 * 1000,
};

async function okxFetch(path, params = {}) {
  const url = new URL(EXCHANGES.okx.base + path);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
    });

    if (response.status === 429) {
      const retryAfter = response.headers.get("Retry-After");
      const retrySeconds = parseFloat(retryAfter);
      await new Promise(r => setTimeout(r, retrySeconds > 0 ? retrySeconds * 1000 : 1000 * (attempt + 1)));
      continue;
    }

    if (!response.ok) {
      throw new Error(`OKX HTTP ${response.status}`);
    }

    const json = await response.json();
    if (json.code !== "0") {
      throw new Error(`OKX error: ${json.msg || "unknown"}`);
    }
    return json;
  }
  throw new Error("OKX خطای نهایی پس از تلاش‌های مکرر");
}

async function genericFetch(baseUrl, path, params = {}) {
  const url = new URL(baseUrl + path);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
    });

    if (response.status === 429) {
      await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
      continue;
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  }
  throw new Error("HTTP خطای نهایی پس از تلاش‌های مکرر");
}

async function toobitFetch(path, params = {}) {
  const url = new URL(EXCHANGES.toobit.base + path);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
    });

    if (response.status === 429) {
      await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
      continue;
    }

    if (!response.ok) {
      throw new Error(`Toobit HTTP ${response.status}`);
    }

    return await response.json();
  }
  throw new Error("Toobit خطای نهایی پس از تلاش‌های مکرر");
}

async function getTopSymbols() {
  const allSymbolsMap = new Map();

  const attempts = CONFIG.ANALYSIS_ORDER.map(async (exchange) => {
    let symbols = [];
    let tickers = [];

    if (exchange === "okx") {
      const json = await okxFetch("/api/v5/public/instruments", { instType: "SWAP" });
      symbols = json.data.filter(item => item.state === "live" && item.settleCcy === "USDT" && item.ctType === "linear" && item.instId.endsWith("-USDT-SWAP")).map(item => {
        const base = item.ctValCcy || item.instId.split("-")[0];
        return { symbol: `${base}USDT`, base, exchange: "okx", raw: item.instId };
      });
      
      const tickerJson = await okxFetch("/api/v5/market/tickers", { instType: "SWAP" });
      tickers = tickerJson.data;
    } else if (exchange === "bybit") {
      const json = await genericFetch(EXCHANGES.bybit.base, "/v5/market/instruments-info", { category: "linear", limit: "1000" });
      symbols = json.result.list.filter(item => item.status === "Trading" && item.quoteCoin === "USDT" && item.contractType === "LinearPerpetual").map(item => ({ symbol: item.symbol, base: item.baseCoin, exchange: "bybit", raw: item.symbol }));

      const tickerJson = await genericFetch(EXCHANGES.bybit.base, "/v5/market/tickers", { category: "linear" });
      tickers = tickerJson.result.list;
    } else if (exchange === "toobit") {
      const json = await toobitFetch("/api/v1/exchangeInfo");
      symbols = (json.contracts || []).filter(item => item.status === "TRADING" && item.inverse === false && item.symbol.endsWith("-SWAP-USDT")).map(item => {
        const raw = item.symbol;
        const base = item.underlying || raw.split("-")[0];
        return { symbol: `${base}USDT`, base, exchange: "toobit", raw };
      });

      const tickerJson = await toobitFetch("/quote/v1/contract/ticker/24hr");
      tickers = Array.isArray(tickerJson) ? tickerJson : tickerJson.data || [];
    }

    if (symbols.length === 0) throw new Error("لیست خالی");

    return { symbols, tickers, exchange };
  });

  try {
    const result = await Promise.any(attempts);
    const { symbols, tickers, exchange } = result;

    const tickerMap = new Map();
    for (const t of tickers) {
      let sym = "";
      let volume = 0;
      if (exchange === "okx") {
        sym = t.instId.replace("-SWAP", "");
        const last = parseFloat(t.last || "0");
        const vol = parseFloat(t.volCcy24h || "0");
        volume = last * vol;
      } else if (exchange === "bybit") {
        sym = t.symbol;
        volume = parseFloat(t.turnover24h || "0");
      } else if (exchange === "toobit") {
        sym = t.s.replace("-SWAP-USDT", "USDT");
        volume = parseFloat(t.qv || "0");
      }
      tickerMap.set(sym, volume);
    }

    const ranked = symbols.map(s => ({
      ...s,
      volume: tickerMap.get(s.symbol) || 0
    })).filter(s => s.volume >= CONFIG.MIN_24H_VOLUME);

    ranked.sort((a, b) => b.volume - a.volume);

    return ranked.slice(0, CONFIG.TOP_SYMBOL_COUNT);
  } catch (e) {
    throw new Error("خطا در دریافت لیست ارزها از تمام صرافی‌ها");
  }
}

async function getCandles(symbol, exchange) {
  const base = symbol.replace(/USDT$/, "");
  const exchangesToTry = [exchange, ...CONFIG.ANALYSIS_ORDER.filter(e => e !== exchange)];

  const attempts = exchangesToTry.map(async (currentExchange) => {
    let candles = [];

    if (currentExchange === "okx") {
      const params = { instId: `${base}-USDT-SWAP`, bar: "15m", limit: String(CONFIG.FETCH_CANDLES) };
      const json = await okxFetch("/api/v5/market/candles", params);
      if (!Array.isArray(json.data)) throw new Error("OKX candle error");
      candles = json.data.map(row => ({ time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), closed: row[8] === "1" }));
    } else if (currentExchange === "bybit") {
      const params = { category: "linear", symbol, interval: "15", limit: String(CONFIG.FETCH_CANDLES) };
      const json = await genericFetch(EXCHANGES.bybit.base, "/v5/market/kline", params);
      if (!Array.isArray(json.result?.list)) throw new Error("Bybit candle error");
      candles = json.result.list.map(row => ({ time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), closed: Number(row[0]) + CONFIG.INTERVAL_MS <= Date.now() }));
    } else if (currentExchange === "toobit") {
      const params = { symbol: `${base}-SWAP-USDT`, interval: "15m", limit: String(CONFIG.FETCH_CANDLES) };
      const json = await toobitFetch("/quote/v1/klines", params);
      if (!Array.isArray(json)) throw new Error("Toobit candle error");
      candles = json.map(row => ({ time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), closed: Number(row[6]) < Date.now() }));
    }

    candles.sort((a, b) => a.time - b.time);
    const closed = candles.filter(c => c.closed && Number.isFinite(c.time) && Number.isFinite(c.open) && Number.isFinite(c.high) && Number.isFinite(c.low) && Number.isFinite(c.close));

    const unique = [];
    const seen = new Set();
    for (const c of closed) {
      if (seen.has(c.time)) continue;
      seen.add(c.time);
      unique.push(c);
    }

    if (unique.length < 30) throw new Error(`داده کافی نیست (${unique.length})`);
    return unique.slice(-CONFIG.FETCH_CANDLES);
  });

  try {
    const result = await Promise.any(attempts);
    return result;
  } catch (e) {
    throw new Error(`داده‌ای برای ${symbol} از هیچ صرافی یافت نشد.`);
  }
}

function isDoji(candle) {
  const range = candle.high - candle.low;
  if (range <= 0) return false;
  const body = Math.abs(candle.close - candle.open);
  const upperWick = candle.high - Math.max(candle.open, candle.close);
  const lowerWick = Math.min(candle.open, candle.close) - candle.low;
  const bodyRatio = body / range;
  if (bodyRatio > CONFIG.DOJI_MAX_BODY_RATIO) return false;
  if (upperWick < 0 || lowerWick < 0) return false;
  const wickSum = upperWick + lowerWick;
  if (wickSum <= 0) return false;
  const wickDifference = Math.abs(upperWick - lowerWick) / wickSum;
  return wickDifference <= CONFIG.WICK_SYMMETRY_TOLERANCE;
}

function atrCalc(candles, period = 14) {
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
  const lastTrs = trs.slice(-period);
  return lastTrs.reduce((a, b) => a + b, 0) / period;
}

function analyzeSymbol(candles) {
  if (candles.length < 30) return null;

  const signals = [];
  const atrValue = atrCalc(candles);
  const firstIndex = Math.max(2, candles.length - CONFIG.HISTORY_CANDLES - 1);

  for (let i = firstIndex; i < candles.length - 1; i++) {
    const preCandle = candles[i - 1];
    const doji = candles[i];
    const confirmation = candles[i + 1];

    if (!isDoji(doji)) continue;

    const preRange = preCandle.high - preCandle.low;
    if (preRange < atrValue) continue;

    const dojiRange = doji.high - doji.low;
    const confirmationRange = confirmation.high - confirmation.low;
    if (!Number.isFinite(dojiRange) || !Number.isFinite(confirmationRange) || dojiRange <= 0) continue;

    const isGreen = confirmation.close > confirmation.open;
    const isRed = confirmation.close < confirmation.open;
    if (!isGreen && !isRed) continue;

    const direction = isGreen ? "LONG" : "SHORT";
    if (confirmationRange < dojiRange * 0.5) continue;

    const entry = confirmation.close;
    const buffer = (entry * CONFIG.SL_BUFFER_PERCENT) / 100;
    const sl = direction === "LONG" ? doji.low - buffer : doji.high + buffer;
    const risk = Math.abs(entry - sl);
    if (risk <= 0) continue;

    const slPercent = (risk / entry) * 100;
    if (slPercent < 0.3) continue;

    let recentHigh = -Infinity;
    let recentLow = Infinity;
    for (let j = Math.max(0, i - 100); j < i; j++) {
      if (candles[j].high > recentHigh) recentHigh = candles[j].high;
      if (candles[j].low < recentLow) recentLow = candles[j].low;
    }

    if (direction === "LONG") {
      if (doji.high > recentHigh * 0.998) continue;
    } else if (direction === "SHORT") {
      if (doji.low < recentLow * 1.002) continue;
    }

    const tp = direction === "LONG" ? entry + (risk * 2) : entry - (risk * 2);

    let tradeStatus = "OPEN";
    for (let j = i + 2; j < candles.length; j++) {
      const c = candles[j];
      if (direction === "LONG") {
        if (c.low <= sl) { tradeStatus = "SL_HIT"; break; }
        if (c.high >= tp) { tradeStatus = "TP_HIT"; break; }
      } else {
        if (c.high >= sl) { tradeStatus = "SL_HIT"; break; }
        if (c.low <= tp) { tradeStatus = "TP_HIT"; break; }
      }
    }

    signals.push({
      direction,
      entry,
      sl,
      risk,
      tp,
      tradeStatus,
      dojiTime: doji.time,
      confirmationTime: confirmation.time,
      currentPrice: candles[candles.length - 1].close,
    });
  }

  if (signals.length === 0) return null;
  return signals[signals.length - 1];
}

function formatPrice(value) {
  if (!Number.isFinite(value)) return "N/A";
  return value.toLocaleString("en-US", {
    maximumFractionDigits: 10,
    useGrouping: false,
  });
}

function formatTime(timestamp) {
  return new Date(timestamp).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function formatSignal(symbol, signal) {
  const isLong = signal.direction === "LONG";
  const directionText = isLong ? "🟢 LONG" : "🔴 SHORT";
  const icon = isLong ? "🟢" : "🔴";
  const label = isLong ? "LONG" : "SHORT";

  const currentPrice = signal.currentPrice;
  let tradeStatusText = "";
  let statusIcon = "";

  if (signal.tradeStatus === "TP_HIT") {
    tradeStatusText = "✅ برنده (تارگت زده شد)";
    statusIcon = "🟢";
  } else if (signal.tradeStatus === "SL_HIT") {
    tradeStatusText = "❌ بازنده (استاپ خورد)";
    statusIcon = "🔴";
  } else {
    let currentR = 0;
    if (signal.risk > 0) {
      const diff = isLong ? (currentPrice - signal.entry) : (signal.entry - currentPrice);
      currentR = diff / signal.risk;
    }
    tradeStatusText = `معامله باز است (R فعلی: ${currentR.toFixed(2)})`;
    statusIcon = "⏳";
  }

  return (
    "📊 <b>تحلیل Doji</b>\n\n" +
    `${statusIcon} <b>${symbol}</b>\n` +
    "⏱ تایم‌فریم: 15m\n" +
    `📌 وضعیت: ${icon} <b>${label}</b>\n\n` +
    `💰 قیمت فعلی: <b>${formatPrice(currentPrice)}</b>\n\n` +
    "🎯 <b>Doji Setup فعال:</b>\n" +
    `📌 نوع: <b>${directionText}</b>\n` +
    `🎯 ورود: <b>${formatPrice(signal.entry)}</b>\n` +
    `🛑 حد ضرر: <b>${formatPrice(signal.sl)}</b>\n` +
    `📏 ریسک: <b>${formatPrice(signal.risk)}</b>\n\n` +
    `🥇 TP: <b>${formatPrice(signal.tp)}</b> (2.00R)\n\n` +
    `${statusIcon} <b>نتیجه: ${tradeStatusText}</b>\n` +
    `🕯 زمان تأیید: ${formatTime(signal.confirmationTime)}\n\n` +
    "━━━━━━━━━━━━━━━━━━━━━━\n" +
    "💡 مدیریت ریسک و سرمایه:\n" +
    "🔸 با تاچ شدن TP، حد ضرر (Stop) را به نقطه ورود (Breakeven) منتقل کنید.\n" +
    "🔸 در صورت نیاز به خروج زودهنگام، به ساختار بازار دقت کنید.\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
    "⚠️ این خروجی تحلیل الگوریتمی است و تضمین سود نیست."
  );
}

async function sendTelegramMessage(token, chatId, text) {
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "HTML",
      disable_web_page_preview: true
    })
  });

  const responseText = await response.text();
  let data = null;
  try { data = JSON.parse(responseText); } catch {}
  if (!response.ok || !data || data.ok !== true) {
    throw new Error(data?.description || `Telegram API error (HTTP ${response.status})`);
  }
}

async function main() {
  const startTime = Date.now();
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.error("❌ Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.");
    process.exit(1);
  }

  console.log("🚀 Starting Doji scan...");

  let symbols = [];
  try {
    symbols = await getTopSymbols();
  } catch (e) {
    console.error("❌ Failed to fetch symbols:", e.message);
    process.exit(1);
  }

  console.log(`📈 Fetched ${symbols.length} symbols to analyze.`);

  let signalCount = 0;

  for (const item of symbols) {
    if (Date.now() - startTime > CONFIG.TOTAL_TIMEOUT_MS) {
      console.log("⏱ Timeout reached, stopping early.");
      break;
    }

    try {
      const candles = await getCandles(item.symbol, item.exchange);
      const signal = analyzeSymbol(candles);

      if (signal) {
        signalCount++;
        const text = formatSignal(item.symbol, signal);
        await sendTelegramMessage(token, chatId, text);
        console.log(`📤 Sent signal for ${item.symbol} (${signal.direction}).`);
      }
    } catch (e) {
      console.error(`⚠️ Error analyzing ${item.symbol}:`, e.message);
    }

    await new Promise(r => setTimeout(r, CONFIG.MESSAGE_DELAY_MS));
  }

  console.log(`✅ Scan complete. Found and sent ${signalCount} signal(s).`);
  process.exit(0);
}

main().catch((e) => {
  console.error("❌ Fatal error:", e.message);
  process.exit(1);
});
