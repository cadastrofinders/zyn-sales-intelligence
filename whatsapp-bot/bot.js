/**
 * ZYN Capital — WhatsApp Bot
 * Responde consultas dos sócios com dados do Painel Executivo.
 *
 * Uso:
 *   node bot.js          → Mostra QR code para conectar
 *   Escaneie com WhatsApp → Bot fica online
 *
 * Comandos:
 *   resumo / painel      → Resumo executivo completo
 *   pipeline             → Pipeline + deals ativos
 *   receita              → Receitas (recebida, confirmada, prevista)
 *   despesa              → Despesas YTD + burn rate
 *   saldo / caixa        → Saldo C6 + runway
 *   leads                → Leads ativos e conversão
 *   deal <nome>          → Busca deal por nome
 *   busca <texto>        → Busca livre em tudo
 *   ajuda / help         → Lista de comandos
 */

const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");
const fs = require("fs");
const path = require("path");

// ── Config ──
const DATA_DIR = path.join(__dirname, "..", "data");
const PIPELINE_FILE = path.join(DATA_DIR, "pipeline.json");
const GESTAO_FILE = path.join(DATA_DIR, "gestao_cache.json");

// Números autorizados (formato: 55DDD9XXXX@c.us)
// Adicione os números dos sócios aqui
const AUTHORIZED = new Set([
  // "5511999999999@c.us",  // Danilo
  // "5511888888888@c.us",  // Renato
  // "5511777777777@c.us",  // Luiz Roberto
]);
// Se vazio, aceita de qualquer número (para teste)
const ALLOW_ALL = AUTHORIZED.size === 0;

// ── Helpers ──
function fmtBR(value) {
  if (!value || isNaN(value)) return "—";
  const v = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (v >= 100) {
    return sign + "R$ " + v.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  }
  return sign + "R$ " + v.toFixed(2).replace(".", ",");
}

function loadPipeline() {
  try {
    return JSON.parse(fs.readFileSync(PIPELINE_FILE, "utf-8"));
  } catch {
    return null;
  }
}

function loadGestao() {
  try {
    return JSON.parse(fs.readFileSync(GESTAO_FILE, "utf-8"));
  } catch {
    return null;
  }
}

function kpis() {
  const g = loadGestao();
  if (!g) return null;

  const rec = g.receitas || [];
  const desp = g.despesas || [];
  const extrato = g.extrato || [];
  const leads = g.leads || [];

  const recRecebida = rec
    .filter((r) => (r.status || "").toLowerCase().includes("recebid"))
    .reduce((s, r) => s + (r.valor_liquido_zyn || r.valor || 0), 0);
  const recConfirmada = rec
    .filter((r) => (r.status || "").toLowerCase().includes("confirm"))
    .reduce((s, r) => s + (r.valor_liquido_zyn || r.valor || 0), 0);
  const recPrevista = rec
    .filter(
      (r) =>
        !(r.status || "").toLowerCase().includes("recebid") &&
        !(r.status || "").toLowerCase().includes("confirm")
    )
    .reduce((s, r) => s + (r.valor_liquido_zyn || r.valor || 0), 0);

  const despPaga = desp
    .filter((d) => (d.status || "").toLowerCase().includes("pag"))
    .reduce((s, d) => s + Math.abs(d.valor || 0), 0);

  const mesesPagos = new Set(
    desp
      .filter((d) => (d.status || "").toLowerCase().includes("pag") && d.data)
      .map((d) => d.data.substring(0, 7))
  ).size;
  const burnRate = mesesPagos > 0 ? despPaga / mesesPagos : 0;

  const saldo = extrato.length
    ? extrato.sort((a, b) => (b.data || "").localeCompare(a.data || ""))[0]
        .saldo || 0
    : 0;

  const runway = burnRate > 0 ? saldo / burnRate : 0;

  const leadsAtivos = leads.filter(
    (l) => !(l.status || "").toLowerCase().includes("convert")
  ).length;
  const leadsConvertidos = leads.filter((l) =>
    (l.status || "").toLowerCase().includes("convert")
  ).length;

  return {
    recRecebida,
    recConfirmada,
    recPrevista,
    despPaga,
    burnRate,
    saldo,
    runway,
    leadsAtivos,
    leadsConvertidos,
    leadsTotal: leads.length,
  };
}

// ── Command handlers ──
function cmdResumo() {
  const pipe = loadPipeline();
  const k = kpis();
  if (!pipe || !k) return "Dados indisponíveis. Rode o sync primeiro.";

  const deals = pipe.deals || [];
  const ativos = deals.filter((d) => d.status !== "Declinado");
  const pipeTotal = ativos.reduce((s, d) => s + (d.valor || 0), 0);
  const quentes = ativos.filter((d) => d.status === "Quente").length;
  const mornos = ativos.filter((d) => d.status === "Morno").length;
  const frios = ativos.filter((d) => d.status === "Frio").length;

  return [
    `*ZYN Capital — Resumo Executivo*`,
    `_${new Date().toLocaleDateString("pt-BR")}_`,
    ``,
    `*Pipeline:* ${fmtBR(pipeTotal)} (${ativos.length} deals)`,
    `  🔴 Quentes: ${quentes} | 🟡 Mornos: ${mornos} | 🔵 Frios: ${frios}`,
    ``,
    `*Receita Recebida:* ${fmtBR(k.recRecebida)}`,
    `*Receita Confirmada:* ${fmtBR(k.recConfirmada)}`,
    `*Receita Prevista:* ${fmtBR(k.recPrevista)}`,
    ``,
    `*Despesas YTD:* ${fmtBR(k.despPaga)}`,
    `*Burn Rate:* ${fmtBR(k.burnRate)}/mês`,
    `*Saldo C6:* ${fmtBR(k.saldo)}`,
    `*Runway:* ${k.runway.toFixed(1)} meses`,
    ``,
    `*Leads:* ${k.leadsAtivos} ativos | ${k.leadsConvertidos} convertidos`,
  ].join("\n");
}

function cmdPipeline() {
  const pipe = loadPipeline();
  if (!pipe) return "Pipeline indisponível.";

  const deals = pipe.deals || [];
  const ativos = deals.filter((d) => d.status !== "Declinado");
  const pipeTotal = ativos.reduce((s, d) => s + (d.valor || 0), 0);

  const lines = [
    `*Pipeline ZYN* — ${ativos.length} deals ativos`,
    `Volume: ${fmtBR(pipeTotal)}`,
    ``,
  ];

  const statusOrder = ["Quente", "Morno", "Frio", "TS Assinado"];
  for (const status of statusOrder) {
    const group = ativos.filter((d) => d.status === status);
    if (group.length === 0) continue;
    const emoji = { Quente: "🔴", Morno: "🟡", Frio: "🔵", "TS Assinado": "✅" }[status] || "⚪";
    lines.push(`*${emoji} ${status}:*`);
    for (const d of group) {
      const nome = (d.cliente || "").substring(0, 25);
      const tipo = d.tipo_operacao || "";
      const val = fmtBR(d.valor);
      lines.push(`  ${nome} — ${tipo} ${val}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

function cmdReceita() {
  const k = kpis();
  if (!k) return "Dados indisponíveis.";
  return [
    `*Receitas ZYN 2026*`,
    ``,
    `✅ *Recebida:* ${fmtBR(k.recRecebida)}`,
    `📋 *Confirmada:* ${fmtBR(k.recConfirmada)}`,
    `📊 *Prevista:* ${fmtBR(k.recPrevista)}`,
    ``,
    `*Total projetado:* ${fmtBR(k.recRecebida + k.recConfirmada + k.recPrevista)}`,
  ].join("\n");
}

function cmdDespesa() {
  const k = kpis();
  if (!k) return "Dados indisponíveis.";
  return [
    `*Despesas ZYN 2026*`,
    ``,
    `💸 *Pagas YTD:* ${fmtBR(k.despPaga)}`,
    `📉 *Burn Rate:* ${fmtBR(k.burnRate)}/mês`,
  ].join("\n");
}

function cmdSaldo() {
  const k = kpis();
  if (!k) return "Dados indisponíveis.";
  return [
    `*Caixa ZYN*`,
    ``,
    `🏦 *Saldo C6:* ${fmtBR(k.saldo)}`,
    `📉 *Burn Rate:* ${fmtBR(k.burnRate)}/mês`,
    `⏳ *Runway:* ${k.runway.toFixed(1)} meses`,
  ].join("\n");
}

function cmdLeads() {
  const k = kpis();
  if (!k) return "Dados indisponíveis.";
  const conv = k.leadsTotal > 0
    ? ((k.leadsConvertidos / k.leadsTotal) * 100).toFixed(0)
    : 0;
  return [
    `*Leads ZYN*`,
    ``,
    `📊 Total: ${k.leadsTotal}`,
    `🟢 Ativos: ${k.leadsAtivos}`,
    `✅ Convertidos: ${k.leadsConvertidos}`,
    `📈 Conversão: ${conv}%`,
  ].join("\n");
}

function cmdDeal(query) {
  const pipe = loadPipeline();
  if (!pipe) return "Pipeline indisponível.";

  const deals = pipe.deals || [];
  const q = query.toLowerCase();
  const found = deals.filter(
    (d) =>
      (d.cliente || "").toLowerCase().includes(q) ||
      (d.tipo_operacao || "").toLowerCase().includes(q)
  );

  if (found.length === 0) return `Nenhum deal encontrado para "${query}".`;

  const lines = [`*Resultado para "${query}":*`, ""];
  for (const d of found.slice(0, 10)) {
    const emoji = { Quente: "🔴", Morno: "🟡", Frio: "🔵", "TS Assinado": "✅", Declinado: "❌" }[d.status] || "⚪";
    lines.push(`${emoji} *${d.cliente}*`);
    lines.push(`  ${d.tipo_operacao || "—"} | ${d.instrumento || "—"} | ${fmtBR(d.valor)}`);
    lines.push(`  Status: ${d.status} | Sócio: ${d.socio || "—"}`);
    if (d.analisando && d.analisando.length) {
      lines.push(`  Analisando: ${d.analisando.join(", ")}`);
    }
    if (d.cobrar_retorno) {
      lines.push(`  Cobrar retorno: ${d.cobrar_retorno}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

function cmdBusca(query) {
  // Search across pipeline + gestao
  const results = [];

  // Pipeline
  const pipe = loadPipeline();
  if (pipe) {
    const q = query.toLowerCase();
    const found = (pipe.deals || []).filter(
      (d) =>
        (d.cliente || "").toLowerCase().includes(q) ||
        (d.tipo_operacao || "").toLowerCase().includes(q) ||
        (d.socio || "").toLowerCase().includes(q) ||
        (d.originador || "").toLowerCase().includes(q) ||
        (d.instrumento || "").toLowerCase().includes(q) ||
        ((d.analisando || []).join(" ").toLowerCase().includes(q))
    );
    if (found.length) {
      results.push(`*Pipeline (${found.length}):*`);
      for (const d of found.slice(0, 5)) {
        results.push(`  ${d.cliente} — ${d.status} — ${fmtBR(d.valor)}`);
      }
      results.push("");
    }
  }

  // Gestao
  const g = loadGestao();
  if (g) {
    const q = query.toLowerCase();
    const recFound = (g.receitas || []).filter(
      (r) =>
        (r.cliente || r.descricao || "").toLowerCase().includes(q)
    );
    if (recFound.length) {
      results.push(`*Receitas (${recFound.length}):*`);
      for (const r of recFound.slice(0, 5)) {
        results.push(
          `  ${r.cliente || r.descricao || "—"} — ${fmtBR(r.valor_liquido_zyn || r.valor)} (${r.status || "—"})`
        );
      }
      results.push("");
    }
  }

  if (results.length === 0) return `Nada encontrado para "${query}".`;
  return [`*Busca: "${query}"*`, "", ...results].join("\n");
}

function cmdAjuda() {
  return [
    `*ZYN Bot — Comandos:*`,
    ``,
    `📊 *resumo* — Resumo executivo completo`,
    `📈 *pipeline* — Pipeline + deals por status`,
    `💰 *receita* — Receitas (recebida/confirmada/prevista)`,
    `💸 *despesa* — Despesas + burn rate`,
    `🏦 *saldo* — Saldo C6 + runway`,
    `🎯 *leads* — Leads ativos e conversão`,
    `🔍 *deal <nome>* — Buscar deal (ex: "deal Giongo")`,
    `🔎 *busca <texto>* — Busca livre`,
    `❓ *ajuda* — Esta mensagem`,
  ].join("\n");
}

// ── Message router ──
function handleMessage(text) {
  const msg = text.trim().toLowerCase();

  if (["resumo", "painel", "report", "relatório", "relatorio"].includes(msg))
    return cmdResumo();
  if (["pipeline", "pipe", "deals"].includes(msg)) return cmdPipeline();
  if (["receita", "receitas", "revenue"].includes(msg)) return cmdReceita();
  if (["despesa", "despesas", "gastos", "custo", "custos"].includes(msg))
    return cmdDespesa();
  if (["saldo", "caixa", "banco", "c6", "runway"].includes(msg))
    return cmdSaldo();
  if (["leads", "lead", "prospecção", "prospeccao"].includes(msg))
    return cmdLeads();
  if (["ajuda", "help", "comandos", "?", "menu"].includes(msg))
    return cmdAjuda();

  if (msg.startsWith("deal ")) return cmdDeal(text.trim().substring(5));
  if (msg.startsWith("busca ") || msg.startsWith("buscar "))
    return cmdBusca(text.trim().split(" ").slice(1).join(" "));

  // Fuzzy: se contém palavras-chave
  if (msg.includes("pipeline") || msg.includes("deal")) return cmdPipeline();
  if (msg.includes("receita")) return cmdReceita();
  if (msg.includes("despesa") || msg.includes("gasto")) return cmdDespesa();
  if (msg.includes("saldo") || msg.includes("caixa") || msg.includes("runway"))
    return cmdSaldo();
  if (msg.includes("lead")) return cmdLeads();

  // Default: tenta buscar
  if (msg.length >= 3) return cmdBusca(text.trim());

  return cmdAjuda();
}

// ── WhatsApp Client ──
const client = new Client({
  authStrategy: new LocalAuth({ dataPath: path.join(__dirname, ".wwebjs_auth") }),
  puppeteer: { headless: true, args: ["--no-sandbox"] },
});

client.on("qr", (qr) => {
  console.log("\n📱 Escaneie o QR code abaixo com o WhatsApp:\n");
  qrcode.generate(qr, { small: true });
  console.log("\nAbra WhatsApp → Dispositivos conectados → Conectar dispositivo\n");
});

client.on("ready", () => {
  console.log("✅ ZYN Bot conectado ao WhatsApp!");
  console.log("   Envie 'ajuda' para ver os comandos.");
  console.log("   Ctrl+C para parar.\n");
});

client.on("message", async (msg) => {
  // Ignore group messages (optional: remove to enable in groups)
  // if (msg.from.includes("@g.us")) return;

  // Check authorization
  if (!ALLOW_ALL && !AUTHORIZED.has(msg.from)) return;

  // Ignore media, stickers, etc
  if (msg.hasMedia || !msg.body) return;

  const response = handleMessage(msg.body);
  if (response) {
    await msg.reply(response);
  }
});

client.on("auth_failure", () => {
  console.error("❌ Falha na autenticação. Delete .wwebjs_auth e tente novamente.");
});

client.on("disconnected", (reason) => {
  console.log("⚠️  Desconectado:", reason);
});

console.log("🚀 Iniciando ZYN Bot...");
client.initialize();
