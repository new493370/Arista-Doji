const EXCHANGES = {
  okx: { name: "OKX", base: "https://www.okx.com" },
  bybit: { name: "Bybit", base: "https://api.bybit.com" },
  toobit: { name: "Toobit", base: "https://api.toobit.com" },
};

const CONFIG = {
  TIMEFRAME: "15m",
  INTERVAL_MS: 15 * 60 * 1000,
  HISTORY_CANDLES: 5,
  FETCH_CANDLES: 130,
  DOJI_MAX_BODY_RATIO: 0.05,
  WICK_SYMMETRY_TOLERANCE: 0.10,
  SL_BUFFER_PERCENT: 0,
  TOP_SYMBOL_COUNT: 9999999999,
  MIN_QUOTE_VOLUME_24H: 500000,
  ANALYSIS_ORDER: ["okx", "bybit", "toobit"],
  API_MAX_RETRIES: 3,
  MESSAGE_DELAY_MS: 1500,
  TOTAL_TIMEOUT_MS: 12 * 60 * 1000,
};

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function okxFetch(path, params = {}) {
  const url = new URL(EXCHANGES.okx.base + path);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });

    if (response.status === 429) {
      const retryAfter = parseFloat(response.headers.get("Retry-After") || "0");
      await sleep(retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1));
      continue;
    }

    if (!response.ok) throw new Error(`OKX HTTP ${response.status}`);

    const json = await response.json();
    if (json.code !== "0") throw new Error(`OKX: ${json.msg || "unknown"}`);
    return json;
  }
  throw new Error("OKX failed after retries");
}

async function bybitFetch(path, params = {}) {
  const url = new URL(EXCHANGES.bybit.base + path);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });

    if (response.status === 429) {
      await sleep(1000 * (attempt + 1));
      continue;
    }

    if (!response.ok) throw new Error(`Bybit HTTP ${response.status}`);

    const json = await response.json();
    if (json.retCode !== 0) throw new Error(`Bybit: ${json.retMsg || "unknown"}`);
    return json;
  }
  throw new Error("Bybit failed after retries");
}

async function toobitFetch(path, params = {}) {
  const url = new URL(EXCHANGES.toobit.base + path);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  for (let attempt = 0; attempt < CONFIG.API_MAX_RETRIES; attempt++) {
    const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });

    if (response.status === 429) {
      await sleep(1000 * (attempt + 1));
      continue;
    }

    if (!response.ok) throw new Error(`Toobit HTTP ${response.status}`);

    return await response.json();
  }
  throw new Error("Toobit failed after retries");
}

async function fetchOKXSymbols() {
  const json = await okxFetch("/api/v5/public/instruments", { instType: "SWAP" });
  return json.data
    .filter(item => item.state === "live" && item.settleCcy === "USDT" && item.ctType === "linear" && item.instId.endsWith("-USDT-SWAP"))
    .map(item => {
      const base = item.ctValCcy || item.instId.split("-")[0];
      return { symbol: `${base}USDT`, base, exchange: "okx", raw: item.instId };
    });
}

async function fetchOKXTickers() {
  const json = await okxFetch("/api/v5/market/tickers", { instType: "SWAP" });
  return json.data
    .filter(item => item.instId.endsWith("-USDT-SWAP"))
    .map(item => {
      const base = item.instId.split("-")[0];
      const volume = parseFloat(item.volCcy24h || "0");
      return { symbol: `${base}USDT`, volume: Number.isFinite(volume) ? volume : 0 };
    });
}

async function fetchBybitSymbols() {
  const json = await bybitFetch("/v5/market/instruments-info", { category: "linear", limit: "1000" });
  return json.result.list
    .filter(item => item.status === "Trading" && item.quoteCoin === "USDT" && item.contractType === "LinearPerpetual")
    .map(item => ({ symbol: item.symbol, base: item.baseCoin, exchange: "bybit", raw: item.symbol }));
}

async function fetchBybitTickers() {
  const json = await bybitFetch("/v5/market/tickers", { category: "linear" });
  return json.result.list
    .filter(item => item.symbol.endsWith("USDT"))
    .map(item => ({ symbol: item.symbol, volume: parseFloat(item.turnover24h || "0") }));
}

async function fetchToobitSymbols() {
  const json = await toobitFetch("/api/v1/exchangeInfo");
  return (json.contracts || [])
    .filter(item => item.status === "TRADING" && item.inverse === false && item.symbol.endsWith("-SWAP-USDT"))
    .map(item => {
      const raw = item.symbol;
      const base = item.underlying || raw.split("-")[0];
      return { symbol: `${base}USDT`, base, exchange: "toobit", raw };
    });
}

async function fetchToobitTickers() {
  const json = await toobitFetch("/quote/v1/contract/ticker/24hr");
  const rows = Array.isArray(json) ? json : (json?.data || json?.result || []);
  return rows
    .map(item => {
      const raw = String(item.s || item.symbol || "");
      const normalized = raw.replace("-SWAP-USDT", "USDT");
      const volume = parseFloat(item.qv || item.quoteVolume || item.volume || "0");
      return { symbol: normalized, volume: Number.isFinite(volume) ? volume : 0 };
    })
    .filter(x => x.symbol && x.volume > 0);
}

async function getTopSymbols() {
  const allCandidates = [];

  const attempts = [
    { name: "okx", fetchSymbols: fetchOKXSymbols, fetchTickers: fetchOKXTickers },
    { name: "bybit", fetchSymbols: fetchBybitSymbols, fetchTickers: fetchBybitTickers },
    { name: "toobit", fetchSymbols: fetchToobitSymbols, fetchTickers: fetchToobitTickers },
  ];

  const order = CONFIG.ANALYSIS_ORDER.map(name => attempts.find(a => a.name === name)).filter(Boolean);

  for (const exchange of order) {
    try {
      console.log(`  Trying ${exchange.name}...`);
      const symbols = await exchange.fetchSymbols();
      const tickers = await exchange.fetchTickers();

      console.log(`  ✅ ${exchange.name}: ${symbols.length} symbols fetched, ${tickers.length} tickers`);

      if (!symbols || symbols.length === 0) {
        console.log(`  ❌ ${exchange.name}: empty symbols list`);
        continue;
      }

      const volumeMap = new Map(tickers.map(t => [t.symbol, t.volume]));

      const ranked = symbols
        .map(s => ({ ...s, volume: volumeMap.get(s.symbol) || 0 }))
        .filter(s => s.volume >= CONFIG.MIN_QUOTE_VOLUME_24H)
        .sort((a, b) => b.volume - a.volume);

      console.log(`  ✅ ${exchange.name}: ${ranked.length} symbols after volume filter (min: ${CONFIG.MIN_QUOTE_VOLUME_24H})`);

      allCandidates.push(...ranked);
    } catch (e) {
      console.log(`  ❌ ${exchange.name}: ${e.message}`);
    }
  }

  if (allCandidates.length === 0) {
    throw new Error("No symbols from any exchange");
  }

  const uniqueMap = new Map();
  for (const item of allCandidates) {
    if (!uniqueMap.has(item.symbol)) {
      uniqueMap.set(item.symbol, item);
    }
  }

  const combined = [...uniqueMap.values()]
    .sort((a, b) => b.volume - a.volume)
    .slice(0, CONFIG.TOP_SYMBOL_COUNT);

  return combined;
}

async function getCandles(symbol, exchange) {
  const base = symbol.replace(/USDT$/, "");
  const exchangesToTry = [exchange, ...CONFIG.ANALYSIS_ORDER.filter(e => e !== exchange)];

  let lastError = null;

  for (const currentExchange of exchangesToTry) {
    try {
      let candles = [];

      if (currentExchange === "okx") {
        const json = await okxFetch("/api/v5/market/candles", {
          instId: `${base}-USDT-SWAP`,
          bar: "15m",
          limit: String(CONFIG.FETCH_CANDLES),
        });
        if (!Array.isArray(json.data)) throw new Error("invalid");
        candles = json.data.map(row => ({
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
        if (!Array.isArray(json.result?.list)) throw new Error("invalid");
        candles = json.result.list.map(row => ({
          time: Number(row[0]),
          open: Number(row[1]),
          high: Number(row[2]),
          low: Number(row[3]),
          close: Number(row[4]),
          closed: Number(row[0]) + CONFIG.INTERVAL_MS + 30000 <= Date.now(),
        }));
      } else if (currentExchange === "toobit") {
        const json = await toobitFetch("/quote/v1/klines", {
          symbol: `${base}-SWAP-USDT`,
          interval: "15m",
          limit: String(CONFIG.FETCH_CANDLES),
        });
        if (!Array.isArray(json)) throw new Error("invalid");
        candles = json.map(row => ({
          time: Number(row[0]),
          open: Number(row[1]),
          high: Number(row[2]),
          low: Number(row[3]),
          close: Number(row[4]),
          closed: Number(row[6]) + 30000 <= Date.now(),
        }));
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

      if (unique.length >= 30) return unique.slice(-CONFIG.FETCH_CANDLES);
      throw new Error(`only ${unique.length} candles`);
    } catch (e) {
      lastError = e;
    }
  }

  throw lastError || new Error("no candles");
}

function dojiType(candle) {
  const range = candle.high - candle.low;
  if (range <= 0) return null;
  const body = Math.abs(candle.close - candle.open);
  if (body / range > CONFIG.DOJI_MAX_BODY_RATIO) return null;
  const upperWick = candle.high - Math.max(candle.open, candle.close);
  const lowerWick = Math.min(candle.open, candle.close) - candle.low;
  if (upperWick < 0 || lowerWick < 0) return null;
  const wickSum = upperWick + lowerWick;
  if (wickSum <= 0) return null;

  const upperRatio = upperWick / range;
  const lowerRatio = lowerWick / range;

  if (upperRatio <= 0.05 && lowerRatio >= 0.60) return "dragonfly";
  if (lowerRatio <= 0.05 && upperRatio >= 0.60) return "gravestone";
  if (Math.abs(upperWick - lowerWick) / wickSum <= CONFIG.WICK_SYMMETRY_TOLERANCE) return "standard";
  return null;
}

function isDoji(candle) {
  return dojiType(candle) !== null;
}

function atrCalc(candles, period = 14) {
  if (!candles || candles.length < period + 1) return 0;
  const trs = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    if (i === 0) {
      trs.push(c.high - c.low);
    } else {
      const pc = candles[i - 1].close;
      trs.push(Math.max(c.high - c.low, Math.abs(c.high - pc), Math.abs(c.low - pc)));
    }
  }
  return trs.slice(-period).reduce((a, b) => a + b, 0) / period;
}

function analyzeSymbol(candles) {
  if (candles.length < 30) return null;

  const atrValue = atrCalc(candles);
  if (!Number.isFinite(atrValue) || atrValue <= 0) return null;

  const lastConfirmIdx = candles.length - 1;
  const firstIndex = Math.max(2, candles.length - CONFIG.HISTORY_CANDLES - 1);

  for (let i = firstIndex; i < lastConfirmIdx; i++) {
    const doji = candles[i];
    const confirmation = candles[i + 1];

    const kind = dojiType(doji);
    if (!kind) continue;

    const dojiRange = doji.high - doji.low;
    const confRange = confirmation.high - confirmation.low;
    if (!Number.isFinite(dojiRange) || !Number.isFinite(confRange) || dojiRange <= 0) continue;
    if (confRange < atrValue * 0.5) continue;

    const confBody = Math.abs(confirmation.close - confirmation.open);
    if (confBody < atrValue * 0.3) continue;

    const isGreen = confirmation.close > confirmation.open;
    const isRed = confirmation.close < confirmation.open;
    if (!isGreen && !isRed) continue;

    const direction = isGreen ? "LONG" : "SHORT";

    if (kind === "dragonfly" && direction !== "LONG") continue;
    if (kind === "gravestone" && direction !== "SHORT") continue;

    let recentHigh = -Infinity, recentLow = Infinity;
    for (let j = Math.max(0, i - 100); j < i; j++) {
      if (candles[j].high > recentHigh) recentHigh = candles[j].high;
      if (candles[j].low < recentLow) recentLow = candles[j].low;
    }
    if (!Number.isFinite(recentHigh) || !Number.isFinite(recentLow)) continue;

    const dojiMid = (doji.high + doji.low) / 2;
    const range100 = recentHigh - recentLow;
    if (range100 <= 0) continue;

    if (direction === "LONG") {
      if (doji.low > recentLow + range100 * 0.35) continue;
      if (doji.high > recentHigh * 0.998) continue;
      if (dojiMid > (recentHigh + recentLow) / 2) continue;
    } else {
      if (doji.high < recentHigh - range100 * 0.35) continue;
      if (doji.low < recentLow * 1.002) continue;
      if (dojiMid < (recentHigh + recentLow) / 2) continue;
    }

    const entry = confirmation.close;
    const buffer = dojiRange * CONFIG.SL_BUFFER_PERCENT;
    const sl = direction === "LONG" ? doji.low - buffer : doji.high + buffer;
    const risk = Math.abs(entry - sl);
    if (risk <= 0) continue;
    if (risk < atrValue * 0.3) continue;

    let structuralTp = direction === "LONG" ? entry + risk * 2 : entry - risk * 2;
    for (let j = i - 1; j >= Math.max(0, i - 100); j--) {
      if (direction === "LONG") {
        const level = candles[j].high;
        if (level > entry + risk * 1.0 && level < structuralTp) structuralTp = level;
      } else {
        const level = candles[j].low;
        if (level < entry - risk * 1.0 && level > structuralTp) structuralTp = level;
      }
    }
    const tp = structuralTp;

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

    if (i + 1 !== lastConfirmIdx) continue;

    return {
      direction, kind, entry, sl, risk, tp, tradeStatus,
      dojiTime: doji.time,
      confirmationTime: confirmation.time,
      currentPrice: candles[candles.length - 1].close,
    };
  }

  return null;
}

function fmtPrice(v) {
  if (!Number.isFinite(v)) return "N/A";
  return v.toLocaleString("en-US", { maximumFractionDigits: 10, useGrouping: false });
}

function fmtTime(ts) {
  return new Date(ts).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function formatSignal(symbol, sig) {
  const isLong = sig.direction === "LONG";
  const dirIcon = isLong ? "🟢" : "🔴";
  const dirText = isLong ? "LONG" : "SHORT";

  let statusText = "", statusIcon = "";
  if (sig.tradeStatus === "TP_HIT") {
    statusText = "برنده (تارگت زده شد)";
    statusIcon = "✅";
  } else if (sig.tradeStatus === "SL_HIT") {
    statusText = "بازنده (استاپ خورد)";
    statusIcon = "❌";
  } else {
    const diff = isLong ? (sig.currentPrice - sig.entry) : (sig.entry - sig.currentPrice);
    const currentR = sig.risk > 0 ? diff / sig.risk : 0;
    statusText = `معامله باز است (R فعلی: ${currentR.toFixed(2)})`;
    statusIcon = "⏳";
  }

  const kindLabel = sig.kind === "dragonfly" ? "Dragonfly"
    : sig.kind === "gravestone" ? "Gravestone"
    : "Standard";

  return (
    "📊 <b>تحلیل Doji</b>\n\n" +
    `${dirIcon} <b>${escapeHtml(symbol)}</b>\n` +
    "⏱ تایم‌فریم: 15m\n" +
    `📌 وضعیت: ${dirIcon} <b>${dirText}</b>\n\n` +
    `💰 قیمت فعلی: <b>${fmtPrice(sig.currentPrice)}</b>\n\n` +
    "🎯 <b>Doji Setup فعال:</b>\n" +
    `🕯 نوع دوجی: <b>${kindLabel}</b>\n` +
    `🎯 ورود: <b>${fmtPrice(sig.entry)}</b>\n` +
    `🛑 حد ضرر: <b>${fmtPrice(sig.sl)}</b>\n` +
    `📏 ریسک: <b>${fmtPrice(sig.risk)}</b>\n\n` +
    `🥇 TP: <b>${fmtPrice(sig.tp)}</b>\n\n` +
    `${statusIcon} <b>نتیجه: ${statusText}</b>\n` +
    `🕯 زمان تأیید: ${fmtTime(sig.confirmationTime)}\n\n` +
    "━━━━━━━━━━━━━━━━━━━━━━\n" +
    "💡 مدیریت ریسک و سرمایه:\n" +
    "🔸 با تاچ شدن TP، حد ضرر را به Breakeven منتقل کنید.\n" +
    "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
    "⚠️ این خروجی تحلیل الگوریتمی است و تضمین سود نیست."
  );
}

async function sendTelegram(token, chatId, text) {
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: "HTML", disable_web_page_preview: true }),
  });
  const data = await res.json();
  if (!data.ok) throw new Error(data.description || `Telegram error HTTP ${res.status}`);
}

async function main() {
  const startTime = Date.now();
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.error("❌ Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID");
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
  if (symbols.length > 0) {
    console.log(`   Top 5: ${symbols.slice(0, 5).map(s => s.symbol).join(", ")}`);
  }
  console.log("");

  let signalCount = 0;

  for (let i = 0; i < symbols.length; i++) {
    if (Date.now() - startTime > CONFIG.TOTAL_TIMEOUT_MS) {
      console.log(`⏱ Timeout reached at symbol ${i + 1}/${symbols.length}`);
      break;
    }

    const item = symbols[i];

    try {
      const candles = await getCandles(item.symbol, item.exchange);
      const signal = analyzeSymbol(candles);

      if (signal) {
        signalCount++;
        const text = formatSignal(item.symbol, signal);
        await sendTelegram(token, chatId, text);
        console.log(`📤 [${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) → ${signal.direction} (${signal.tradeStatus})`);
      } else {
        console.log(`⚪ [${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) → no doji`);
      }
    } catch (e) {
      console.log(`⚠️ [${i + 1}/${symbols.length}] ${item.symbol} (${item.exchange}) → ${e.message}`);
    }

    await sleep(CONFIG.MESSAGE_DELAY_MS);
  }

  console.log("");
  console.log(`✅ Scan complete. Found and sent ${signalCount} signal(s).`);
  process.exit(0);
}

main().catch((e) => {
  console.error("❌ Fatal error:", e.message);
  process.exit(1);
});
