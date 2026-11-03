#!/usr/bin/env node
/**
 * NodePusher —— 每天从 GitHub 抓取免费节点，实测筛选后生成 Clash yaml 并推送
 *
 * 启动: node app.js
 * 面板: http://127.0.0.1:8787
 *
 * 只依赖 Node 内置模块 + js-yaml（vendor 目录）+ mihomo 内核（bin 目录）
 */
"use strict";

const http = require("node:http");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const net = require("node:net");
const { spawn } = require("node:child_process");
const yaml = require("./vendor/js-yaml");

const ROOT = __dirname;
const DATA = path.join(ROOT, "data");
const RUNS = path.join(DATA, "runs");
const WORK = path.join(DATA, "work");
const CONFIG_PATH = path.join(ROOT, "config.json");

/* ---------------------------------------------------------------- 配置 */

function loadConfig() {
  const raw = fs.readFileSync(CONFIG_PATH, "utf8");
  return JSON.parse(raw);
}

let CFG = loadConfig();
if (process.env.NODEPUSHER_CORE) CFG.core.binPath = process.env.NODEPUSHER_CORE;
if (process.env.NODEPUSHER_PORT) CFG.port = Number(process.env.NODEPUSHER_PORT);

/* ---------------------------------------------------------------- 状态 */

const state = {
  status: "idle", // idle | running | ok | failed
  step: "",
  stepIndex: 0,
  stepTotal: 8,
  detail: "",
  startedAt: null,
  finishedAt: null,
  lastResult: null,
  nextRunAt: null,
  logs: [],
  counts: {},
};

function log(msg) {
  const line = `[${new Date().toLocaleTimeString("zh-CN", { hour12: false })}] ${msg}`;
  state.logs.push(line);
  if (state.logs.length > 400) state.logs.shift();
  console.log(line);
}

function setStep(index, name, detail = "") {
  state.status = "running";
  state.stepIndex = index;
  state.step = name;
  state.detail = detail;
  log(`▶ ${index}/${state.stepTotal} ${name}${detail ? " — " + detail : ""}`);
}

/* ---------------------------------------------------------------- 工具 */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pool(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  let done = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const k = i++;
      if (k >= items.length) return;
      try {
        out[k] = await fn(items[k], k);
      } catch {
        out[k] = null;
      }
      done++;
      if (done % 50 === 0) state.detail = `${done}/${items.length}`;
    }
  });
  await Promise.all(workers);
  return out;
}

function tcpProbe(host, port, timeout) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const sock = new net.Socket();
    let settled = false;
    const fin = (v) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch {}
      resolve(v);
    };
    sock.setTimeout(timeout);
    sock.once("connect", () => fin(Date.now() - t0));
    sock.once("timeout", () => fin(null));
    sock.once("error", () => fin(null));
    try {
      sock.connect(port, host);
    } catch {
      fin(null);
    }
  });
}

async function httpGet(url, timeoutMs) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(timeoutMs),
    headers: { "User-Agent": "NodePusher/1.0 (+clash yaml collector)" },
  });
  return res;
}

/* ---------------------------------------------------------------- 步骤 1 抓取 */

async function fetchSources() {
  const docs = [];
  let ok = 0;
  for (const src of CFG.sources) {
    if (src.disabled) continue;
    const urls = [src.url, ...(CFG.mirrors || []).map((m) => m + src.url)];
    let got = false;
    for (const url of urls) {
      for (let attempt = 0; attempt < 2 && !got; attempt++) {
        try {
          const res = await httpGet(url, CFG.limits.fetchTimeoutMs);
          if (!res.ok) {
            log(`  ${src.name}: HTTP ${res.status}`);
            break;
          }
          const text = await res.text();
          const doc = yaml.load(text, { json: true });
          const list = (doc && doc.proxies) || [];
          if (!Array.isArray(list) || list.length === 0) {
            log(`  ${src.name}: 解析到 0 个节点`);
            break;
          }
          docs.push({ name: src.name, proxies: list });
          ok++;
          got = true;
          log(`  ${src.name}: ${list.length} 个节点`);
        } catch (e) {
          if (attempt === 1) log(`  ${src.name}: 失败 (${e.cause?.code || e.name})`);
          await sleep(800);
        }
      }
    }
  }
  if (ok === 0) throw new Error("所有订阅源都抓取失败（网络被墙或源已失效）");
  return docs;
}

/* ---------------------------------------------------------------- 步骤 2 去重 */

function mergeDocs(docs) {
  const bad = /^(127\.|10\.|192\.168\.|0\.0\.0\.0|localhost|::1)/i;
  const seen = new Set();
  const out = [];
  for (const doc of docs) {
    for (const p of doc.proxies) {
      if (!p || !p.server || !p.port || !p.type) continue;
      if (bad.test(String(p.server))) continue;
      if (/剩余|到期|官网|流量|过期|订阅|expire|traffic/i.test(String(p.name || ""))) continue;
      const key = `${p.type}|${p.server}|${p.port}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ ...p, _src: doc.name });
    }
  }
  return out;
}

/* ---------------------------------------------------------------- 步骤 3 TCP */

async function tcpFilter(nodes) {
  const { tcpTimeoutMs, tcpConcurrency } = CFG.limits;
  const lat = await pool(nodes, tcpConcurrency, (n) => tcpProbe(n.server, n.port, tcpTimeoutMs));
  const alive = [];
  for (let i = 0; i < nodes.length; i++) if (lat[i] !== null) alive.push({ ...nodes[i], _tcp: lat[i] });
  return alive;
}

/* ---------------------------------------------------------------- 内核控制 */

class Core {
  constructor(bin, cfgFile, workDir, apiPort, mixedPort) {
    this.bin = bin;
    this.cfgFile = cfgFile;
    this.workDir = workDir;
    this.apiPort = apiPort;
    this.mixedPort = mixedPort;
    this.proc = null;
    this.logPath = path.join(workDir, "core.log");
  }

  async start() {
    const out = fs.openSync(this.logPath, "a");
    this.proc = spawn(this.bin, ["-f", this.cfgFile, "-d", this.workDir], {
      stdio: ["ignore", out, out],
      windowsHide: true,
    });
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      await sleep(400);
      try {
        const r = await fetch(`http://127.0.0.1:${this.apiPort}/version`, {
          headers: { Authorization: `Bearer ${CFG.core.secret}` },
          signal: AbortSignal.timeout(1500),
        });
        // 200 正常；401 说明 API 已监听（只是 token 不匹配配置）
        if (r.ok || r.status === 401) return true;
      } catch {}
      if (this.proc.exitCode !== null) throw new Error(`内核启动失败，见 ${this.logPath}`);
    }
    this.stop();
    throw new Error("内核启动超时");
  }

  api(p, opts = {}) {
    return fetch(`http://127.0.0.1:${this.apiPort}${p}`, {
      ...opts,
      headers: { Authorization: `Bearer ${CFG.core.secret}`, "Content-Type": "application/json", ...(opts.headers || {}) },
      signal: AbortSignal.timeout(20000),
    });
  }

  async delay(name, timeoutMs) {
    const url = CFG.limits.testUrl;
    try {
      const r = await this.api(
        `/proxies/${encodeURIComponent(name)}/delay?timeout=${timeoutMs}&url=${encodeURIComponent(url)}`
      );
      if (!r.ok) return null;
      const j = await r.json();
      return typeof j.delay === "number" ? j.delay : null;
    } catch {
      return null;
    }
  }

  async setMode(mode) {
    try {
      await this.api("/configs", { method: "PATCH", body: JSON.stringify({ mode }) });
    } catch {}
  }

  async select(name) {
    try {
      const r = await this.api("/proxies/GLOBAL", { method: "PUT", body: JSON.stringify({ name }) });
      return r.ok;
    } catch {
      return false;
    }
  }

  /** 通过混合端口下载固定字节，返回 {kbps, ttfb, status} */
  download(windowMs = 5000) {
    const port = this.mixedPort;
    return new Promise((resolve) => {
      const t0 = Date.now();
      let body = 0;
      let headerDone = false;
      let ttfb = null;
      let pending = [];
      let status = null;
      let settled = false;
      const sock = net.connect(port, "127.0.0.1");
      const fin = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try { sock.destroy(); } catch {}
        resolve({ kbps: Math.round(body / 1024 / (Math.max(1, Date.now() - t0) / 1000)), ttfb, status, bytes: body });
      };
      const timer = setTimeout(fin, windowMs);
      sock.on("connect", () =>
        sock.write(
          `GET http://speed.cloudflare.com/__down?bytes=20000000 HTTP/1.1\r\nHost: speed.cloudflare.com\r\n` +
            `User-Agent: NodePusher/1.0\r\nConnection: close\r\n\r\n`
        )
      );
      sock.on("data", (c) => {
        if (ttfb === null) ttfb = Date.now() - t0;
        if (headerDone) {
          body += c.length;
          if (body > 3_000_000) fin();
          return;
        }
        pending.push(c);
        const buf = Buffer.concat(pending);
        const i = buf.indexOf("\r\n\r\n");
        if (i >= 0) {
          if (status === null) {
            const m = /^HTTP\/1\.[01] (\d{3})/.exec(buf.subarray(0, 64).toString("latin1"));
            status = m ? Number(m[1]) : 0;
          }
          headerDone = true;
          body = buf.length - (i + 4);
          pending = [];
        }
      });
      sock.on("error", fin);
      sock.on("close", fin);
    });
  }

  stop() {
    if (!this.proc || this.proc.exitCode !== null) return;
    try {
      spawn("taskkill", ["/PID", String(this.proc.pid), "/T", "/F"], { stdio: "ignore", windowsHide: true });
    } catch {
      try { this.proc.kill("SIGKILL"); } catch {}
    }
    this.proc = null;
  }
}

/* ---------------------------------------------------------------- 步骤 5/6 */

async function measure(nodes) {
  const { delayTimeoutMs, delayConcurrency, delayRounds, speedWindowMs, maxSpeedTest, speedConcurrency } = CFG.limits;
  await fsp.mkdir(WORK, { recursive: true });

  const named = nodes.map((n, i) => ({ ...n, name: `${n.type}-${i}-${String(n.name || "node").slice(0, 28)}` }));
  const cfg = {
    "mixed-port": CFG.core.mixedPort,
    "allow-lan": false,
    mode: "rule",
    "log-level": "warning",
    "external-controller": `127.0.0.1:${CFG.core.apiPort}`,
    secret: CFG.core.secret,
    proxies: named.map(({ _src, _tcp, _id, ...p }) => p),
    "proxy-groups": [{ name: "GLOBAL", type: "select", proxies: named.map((n) => n.name) }],
    rules: ["MATCH,DIRECT"],
  };
  // GLOBAL 是内置选择器，不能自定义，改成普通组
  cfg["proxy-groups"] = [{ name: "TEST", type: "select", proxies: named.map((n) => n.name) }];

  const cfgFile = path.join(WORK, "active.yaml");
  await fsp.writeFile(cfgFile, yaml.dump(cfg, { lineWidth: -1, noRefs: true }), "utf8");

  const core = new Core(CFG.core.binPath, cfgFile, WORK, CFG.core.apiPort, CFG.core.mixedPort);
  state.detail = "内核启动中";
  await core.start();
  try {
    // 多轮延迟
    let live = named;
    for (let round = 1; round <= delayRounds; round++) {
      const lat = await pool(live, delayConcurrency, (n) => core.delay(n.name, delayTimeoutMs));
      live = live.map((n, i) => ({ ...n, [`_d${round}`]: lat[i] })).filter((n) => n[`_d${round}`] !== null);
      log(`  延迟第 ${round} 轮：${live.length}/${named.length} 存活`);
      if (live.length === 0) break;
      await sleep(1200);
    }
    if (live.length === 0) return { nodes: [], delayCount: 0, core };

    for (const n of live) {
      const ds = [n._d1, n._d2, n._d3].filter((x) => typeof x === "number");
      n._avg = Math.round(ds.reduce((a, b) => a + b, 0) / ds.length);
      n._jitter = Math.max(...ds) - Math.min(...ds);
    }
    live.sort((a, b) => a._avg - b._avg);

    // 下载测速：优先测延迟最低的一批
    const candidates = live.slice(0, Math.min(maxSpeedTest, live.length));
    await core.setMode("global");
    const tested = [];
    let i = 0;
    const queue = [...candidates];
    await pool(queue, speedConcurrency, async (n) => {
      const ok = await core.select(n.name);
      if (!ok) return null;
      const d = await core.download(speedWindowMs);
      i++;
      state.detail = `测速 ${i}/${queue.length}`;
      if (d.bytes > 80000 && (d.status === 200 || d.status === 206)) {
        tested.push({ ...n, _kbps: d.kbps, _ttfb: d.ttfb });
      }
      return null;
    });
    await core.setMode("rule");
    tested.sort((a, b) => b._kbps - a._kbps);
    log(`  测速完成：${tested.length}/${candidates.length} 个可用`);
    return { nodes: tested, delayCount: live.length, core };
  } finally {
    core.stop();
  }
}

/* ---------------------------------------------------------------- 步骤 7 生成 */

const FLAGS = {
  香港: "🇭🇰", 台湾: "🇹🇼", 日本: "🇯🇵", 韩国: "🇰🇷", 新加坡: "🇸🇬", 美国: "🇺🇸", 英国: "🇬🇧",
  德国: "🇩🇪", 法国: "🇫🇷", 荷兰: "🇳🇱", 意大利: "🇮🇹", 西班牙: "🇪🇸", 波兰: "🇵🇱", 俄罗斯: "🇷🇺",
  拉脱维亚: "🇱🇻", 丹麦: "🇩🇰", 爱尔兰: "🇮🇪", 瑞士: "🇨🇭", 瑞典: "🇸🇪", 加拿大: "🇨🇦", 澳大利亚: "🇦🇺",
  爱沙尼亚: "🇪🇪", 芬兰: "🇫🇮", 挪威: "🇳🇴", 捷克: "🇨🇿", 奥地利: "🇦🇹", 罗马尼亚: "🇷🇴", 葡萄牙: "🇵🇹",
  比利时: "🇧🇪", 巴西: "🇧🇷", 土耳其: "🇹🇷", 印度: "🇮🇳", 中国: "🇨🇳",
};
const CODES = {
  HK: "香港", TW: "台湾", JP: "日本", KR: "韩国", SG: "新加坡", US: "美国", GB: "英国", UK: "英国",
  DE: "德国", FR: "法国", NL: "荷兰", IT: "意大利", ES: "西班牙", PL: "波兰", RU: "俄罗斯", LV: "拉脱维亚",
  DK: "丹麦", IE: "爱尔兰", CH: "瑞士", SE: "瑞典", CA: "加拿大", AU: "澳大利亚", EE: "爱沙尼亚",
  FI: "芬兰", NO: "挪威", CZ: "捷克", AT: "奥地利", RO: "罗马尼亚", PT: "葡萄牙", BE: "比利时",
  BR: "巴西", TR: "土耳其", IN: "印度", CN: "中国",
};
const TYPE_NAME = { vless: "VLESS", vmess: "VMess", trojan: "Trojan", ss: "SS", http: "HTTP", socks5: "SOCKS5", hysteria2: "HY2", hysteria: "HY", anytls: "AnyTLS", tuic: "TUIC" };

function countryOf(name) {
  const s = String(name || "");
  for (const [cn, fl] of Object.entries(FLAGS)) if (s.includes(cn)) return cn;
  for (const [cn, fl] of Object.entries(FLAGS)) if (s.includes(fl)) return cn;
  const m = s.toUpperCase().match(/(?:^|[^A-Z])(HK|TW|JP|KR|SG|US|GB|UK|DE|FR|NL|IT|ES|PL|RU|LV|DK|IE|CH|SE|CA|AU|EE|FI|NO|CZ|AT|RO|PT|BE|BR|TR|IN|CN)(?:[^A-Z]|$)/);
  return m && CODES[m[1]] ? CODES[m[1]] : "其它";
}

function buildClash(nodes, meta) {
  const encTypes = new Set(["vless", "vmess", "trojan", "ss", "hysteria2", "hysteria", "anytls", "tuic", "socks5"]);
  const enc = nodes.filter((n) => encTypes.has(n.type)).slice(0, CFG.output.maxEncrypted);
  const plain = nodes.filter((n) => !encTypes.has(n.type)).slice(0, CFG.output.maxPlain);
  const chosen = [...enc, ...plain];

  const counters = {};
  const used = new Set();
  const proxies = [];
  const info = [];
  for (const n of chosen) {
    const { _src, _tcp, _d1, _d2, _d3, _avg, _jitter, _kbps, _ttfb, ...clean } = n;
    const cn = countryOf(clean.name);
    const flag = FLAGS[cn] || "🌐";
    counters[cn] = (counters[cn] || 0) + 1;
    const mb = _kbps / 1024;
    const sp = mb >= 0.95 ? `${mb.toFixed(1)}MB/s` : `${Math.round(_kbps)}KB/s`;
    let name = `${flag} ${cn} ${String(counters[cn]).padStart(2, "0")} ${TYPE_NAME[clean.type] || clean.type} ${sp}`;
    let k = 2;
    while (used.has(name)) name = `${flag} ${cn} ${String(counters[cn]).padStart(2, "0")} ${TYPE_NAME[clean.type] || clean.type} ${sp}#${k++}`;
    used.add(name);
    proxies.push({ ...clean, name });
    info.push({ cn, flag, type: clean.type, kbps: _kbps, avg: _avg, jitter: _jitter, name });
  }

  const names = proxies.map((p) => p.name);
  const encNames = info.filter((i) => encTypes.has(i.type)).map((i) => i.name);
  const plainNames = info.filter((i) => !encTypes.has(i.type)).map((i) => i.name);
  const regionFlags = [...new Set(info.map((i) => i.flag))].filter((f) => info.filter((i) => i.flag === f).length >= 2);

  const groups = [
    { name: "🚀 节点选择", type: "select", proxies: ["♻️ 自动选择", "🔯 故障转移", "🔒 加密节点", "⚡ 明文节点", "🎯 全球直连", ...names] },
    { name: "♻️ 自动选择", type: "url-test", url: "http://www.gstatic.com/generate_204", interval: 600, tolerance: 50, proxies: names },
    { name: "🔯 故障转移", type: "fallback", url: "http://www.gstatic.com/generate_204", interval: 600, proxies: names },
  ];
  if (encNames.length) groups.push({ name: "🔒 加密节点", type: "select", proxies: ["♻️ 自动选择", ...encNames] });
  if (plainNames.length) groups.push({ name: "⚡ 明文节点", type: "select", proxies: ["♻️ 自动选择", ...plainNames] });
  for (const f of regionFlags) {
    groups.push({ name: `${f} 节点`, type: "select", proxies: ["♻️ 自动选择", ...info.filter((i) => i.flag === f).map((i) => i.name)] });
  }
  groups.push({ name: "🎯 全球直连", type: "select", proxies: ["DIRECT"] });
  groups.push({ name: "🐟 漏网之鱼", type: "select", proxies: ["🚀 节点选择", "♻️ 自动选择", "🎯 全球直连"] });

  const cnDomains = ["cn", "baidu.com", "qq.com", "tencent.com", "weixin.qq.com", "taobao.com", "tmall.com", "alicdn.com",
    "aliyun.com", "alipay.com", "jd.com", "360.cn", "sina.com.cn", "weibo.com", "bilibili.com", "zhihu.com", "163.com",
    "126.net", "netease.com", "douyin.com", "bytedance.com", "iqiyi.com", "youku.com", "csdn.net", "gitee.com",
    "mi.com", "xiaomi.com", "huawei.com", "honor.com", "meituan.com", "dianping.com", "ctrip.com", "12306.cn"];
  const rules = [
    "DOMAIN-SUFFIX,local,DIRECT",
    "IP-CIDR,127.0.0.0/8,DIRECT,no-resolve", "IP-CIDR,10.0.0.0/8,DIRECT,no-resolve",
    "IP-CIDR,172.16.0.0/12,DIRECT,no-resolve", "IP-CIDR,192.168.0.0/16,DIRECT,no-resolve",
    "IP-CIDR,100.64.0.0/10,DIRECT,no-resolve", "IP-CIDR,224.0.0.0/4,DIRECT,no-resolve",
    "IP-CIDR6,fe80::/10,DIRECT,no-resolve",
    ...cnDomains.map((d) => `DOMAIN-SUFFIX,${d},DIRECT`),
    "MATCH,🐟 漏网之鱼",
  ];

  const head = `# ===============================================================
# Clash / mihomo 免费节点（NodePusher 自动生成）
# 生成时间 : ${new Date().toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })} (UTC+8)
# 节点数量 : ${proxies.length} 个，全部经过真实下载测速（>=${CFG.output.minKbps}KB/s）
# 筛选链路 : 抓取 ${meta.raw} 个 -> 去重 ${meta.dedup} -> TCP 可连 ${meta.tcp} -> 延迟存活 ${meta.delay} -> 测速通过 ${meta.tested}
# 数据来源 : ${meta.sources.join("、")}
# 安全提示 : ⚡ 明文节点（HTTP/SOCKS5）不加密，敏感操作用「🔒 加密节点」
# 免责声明 : 节点来自公开网络，仅供学习与测试，请遵守当地法律法规
# ===============================================================
`;
  const cfg = {
    "mixed-port": CFG.output.mixedPort,
    "allow-lan": false,
    mode: "rule",
    "log-level": "info",
    ipv6: false,
    "unified-delay": true,
    "tcp-concurrent": true,
    "external-controller": "127.0.0.1:9090",
    profile: { "store-selected": true, "store-fake-ip": true },
    dns: {
      enable: true, ipv6: false, "enhanced-mode": "fake-ip", "fake-ip-range": "198.18.0.1/16",
      "fake-ip-filter": ["*.lan", "*.local", "localhost.ptlogin2.qq.com"],
      "default-nameserver": ["223.5.5.5", "119.29.29.29"],
      nameserver: ["223.5.5.5", "119.29.29.29", "https://dns.alidns.com/dns-query", "https://doh.pub/dns-query"],
    },
    proxies,
    "proxy-groups": groups,
    rules,
  };
  return { text: head + yaml.dump(cfg, { lineWidth: -1, noRefs: true }), count: proxies.length, info };
}

/* ---------------------------------------------------------------- 步骤 8 推送 */

async function pushLatest(file, summary) {
  const cfg = CFG.push || {};
  const title = `今日节点已更新（${summary.count} 个）`;
  const body = `最快 ${summary.topSpeed} · ${summary.liveSources}`;
  if (cfg.webhook) {
    try {
      const url = cfg.webhook.replace("{title}", encodeURIComponent(title)).replace("{body}", encodeURIComponent(body));
      await fetch(cfg.webhook.includes("{") ? url : cfg.webhook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title, body, file: path.basename(file), count: summary.count }),
        signal: AbortSignal.timeout(10000),
      });
      log("  已推送 Webhook");
    } catch (e) {
      log(`  Webhook 失败: ${e.message}`);
    }
  }
  if (cfg.toast && process.platform === "win32") {
    try {
      const ps = [
        "-NoProfile",
        "-Command",
        "Add-Type -AssemblyName System.Windows.Forms;",
        "$n = New-Object System.Windows.Forms.NotifyIcon;",
        "$n.Icon = [System.Drawing.SystemIcons]::Information;",
        "$n.Visible = $true;",
        `$n.ShowBalloonTip(6000, '${title}', '${body}', [System.Windows.Forms.ToolTipIcon]::Info);`,
        "Start-Sleep -Seconds 7; $n.Dispose()",
      ];
      const child = spawn("powershell", ps, { stdio: "ignore", windowsHide: true, detached: true });
      child.on("error", () => {});
      child.unref();
      log("  已弹出桌面通知");
    } catch (e) {
      log(`  通知失败: ${e.message}`);
    }
  }
}

/* ---------------------------------------------------------------- 主流程 */

async function runPipeline(trigger = "schedule") {
  if (state.status === "running") return { ok: false, error: "已有任务在运行" };
  Object.assign(state, {
    status: "running", logs: [], startedAt: new Date().toISOString(), finishedAt: null,
    step: "准备", stepIndex: 0, detail: "", counts: {},
  });
  const started = Date.now();
  try {
    log(`任务开始（${trigger}）`);

    setStep(1, "抓取订阅源");
    const docs = await fetchSources();
    const raw = docs.reduce((a, d) => a + d.proxies.length, 0);
    state.counts.raw = raw;

    setStep(2, "去重合并");
    const merged = mergeDocs(docs);
    state.counts.dedup = merged.length;
    log(`  去重后 ${merged.length} 个节点`);
    if (merged.length === 0) throw new Error("去重后没有有效节点");

    setStep(3, "TCP 连通性探测", `${merged.length} 个待测`);
    const alive = await tcpFilter(merged);
    state.counts.tcp = alive.length;
    log(`  可连 ${alive.length}/${merged.length}`);
    if (alive.length === 0) throw new Error("没有 TCP 可连的节点（当前网络可能整体受限）");

    setStep(4, "启动内核");
    if (!fs.existsSync(CFG.core.binPath)) throw new Error(`找不到内核: ${CFG.core.binPath}`);

    setStep(5, "真实延迟复测");
    const { nodes: measured, delayCount } = await measure(alive);
    state.counts.delay = delayCount;
    if (measured.length === 0) throw new Error("没有通过真实请求的节点");

    setStep(6, "下载测速");
    const good = measured.filter((n) => n._kbps >= CFG.output.minKbps);
    state.counts.tested = good.length;
    if (good.length === 0) throw new Error(`没有节点达到 ${CFG.output.minKbps}KB/s`);

    setStep(7, "生成 yaml");
    const meta = {
      raw, dedup: merged.length, tcp: alive.length, delay: delayCount,
      tested: good.length, sources: docs.map((d) => d.name),
    };
    const { text, count, info } = buildClash(good, meta);
    if (count === 0) throw new Error("筛选后没有节点");

    const stamp = new Date();
    const fname = `nodes-${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}-${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}.yaml`;
    await fsp.mkdir(RUNS, { recursive: true });
    const outFile = path.join(RUNS, fname);
    await fsp.writeFile(outFile, text, "utf8");
    await fsp.writeFile(path.join(DATA, "latest.yaml"), text, "utf8");

    const top = info[0];
    const summary = {
      count,
      topSpeed: top ? (top.kbps >= 1024 ? `${(top.kbps / 1024).toFixed(1)}MB/s` : `${top.kbps}KB/s`) : "-",
      liveSources: `${docs.length} 个源`,
      file: fname,
      nodes: info,
    };
    await fsp.writeFile(path.join(DATA, "latest.json"), JSON.stringify({ ...summary, at: stamp.toISOString(), meta }, null, 2), "utf8");

    setStep(8, "推送通知", `${count} 个节点`);
    await pushLatest(outFile, summary);

    state.lastResult = { ...summary, meta, durationMs: Date.now() - started };
    state.status = "ok";
    state.step = "完成";
    log(`✔ 完成：${count} 个节点 -> ${fname}（耗时 ${((Date.now() - started) / 1000).toFixed(1)}s）`);
    await cleanupOldRuns();
    return { ok: true, file: fname, count };
  } catch (e) {
    state.status = "failed";
    state.step = "失败";
    state.detail = e.message;
    log(`✘ 失败：${e.message}`);
    return { ok: false, error: e.message };
  } finally {
    state.finishedAt = new Date().toISOString();
    scheduleNext();
  }
}

async function cleanupOldRuns() {
  const keep = Math.max(1, CFG.output.keepRuns || 7);
  try {
    const files = (await fsp.readdir(RUNS)).filter((f) => f.endsWith(".yaml")).sort().reverse();
    for (const f of files.slice(keep)) await fsp.unlink(path.join(RUNS, f));
  } catch {}
}

/* ---------------------------------------------------------------- 调度 */

let nextTimer = null;

function scheduleNext() {
  if (nextTimer) clearTimeout(nextTimer);
  const { enabled, time } = CFG.schedule;
  if (!enabled) {
    state.nextRunAt = null;
    return;
  }
  const [hh, mm] = String(time || "09:00").split(":").map(Number);
  const now = new Date();
  const next = new Date(now);
  next.setHours(hh, mm, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  state.nextRunAt = next.toISOString();
  nextTimer = setTimeout(async () => {
    const r = await runPipeline("schedule");
    if (!r.ok) {
      // 失败后 30 分钟重试，最多 3 次
      state.retry = (state.retry || 0) + 1;
      if (state.retry <= 3) {
        log(`任务失败，30 分钟后重试（${state.retry}/3）`);
        nextTimer = setTimeout(() => runPipeline("retry"), 30 * 60 * 1000);
        state.nextRunAt = new Date(Date.now() + 30 * 60 * 1000).toISOString();
        return;
      }
    }
    state.retry = 0;
    scheduleNext();
  }, next - now);
  log(`下次运行：${next.toLocaleString("zh-CN", { hour12: false })}`);
}

/* ---------------------------------------------------------------- HTTP 服务 */

const MIME = { ".html": "text/html; charset=utf-8", ".yaml": "text/yaml; charset=utf-8", ".json": "application/json; charset=utf-8", ".css": "text/css", ".js": "text/javascript" };

function startServer() {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, "http://localhost");
    const p = url.pathname;

    if (p === "/api/status") {
      res.writeHead(200, { "Content-Type": MIME[".json"] });
      return res.end(JSON.stringify({ ...state, config: { schedule: CFG.schedule, sources: CFG.sources.map((s) => s.name), limits: CFG.limits } }));
    }
    if (p === "/api/run" && req.method === "POST") {
      res.writeHead(202, { "Content-Type": MIME[".json"] });
      res.end(JSON.stringify({ ok: true }));
      runPipeline("manual");
      return;
    }
    if (p === "/api/runs") {
      const files = (await fsp.readdir(RUNS).catch(() => [])).filter((f) => f.endsWith(".yaml")).sort().reverse();
      const stats = [];
      for (const f of files.slice(0, 30)) {
        const st = await fsp.stat(path.join(RUNS, f));
        stats.push({ file: f, size: st.size, mtime: st.mtime.toISOString() });
      }
      res.writeHead(200, { "Content-Type": MIME[".json"] });
      return res.end(JSON.stringify(stats));
    }
    if (p.startsWith("/runs/")) {
      const f = path.basename(p.slice(6));
      const full = path.join(RUNS, f);
      if (fs.existsSync(full)) {
        res.writeHead(200, { "Content-Type": MIME[".yaml"], "Content-Disposition": `attachment; filename="${f}"` });
        return res.end(await fsp.readFile(full));
      }
      res.writeHead(404);
      return res.end("not found");
    }
    if (p === "/latest.yaml") {
      const full = path.join(DATA, "latest.yaml");
      if (fs.existsSync(full)) {
        res.writeHead(200, { "Content-Type": MIME[".yaml"], "Content-Disposition": 'attachment; filename="clash.yaml"' });
        return res.end(await fsp.readFile(full));
      }
      res.writeHead(404);
      return res.end("还没有生成过");
    }

    const file = p === "/" ? "index.html" : path.basename(p);
    const full = path.join(ROOT, "public", file);
    if (fs.existsSync(full)) {
      res.writeHead(200, { "Content-Type": MIME[path.extname(full)] || "application/octet-stream" });
      return res.end(await fsp.readFile(full));
    }
    res.writeHead(404);
    res.end("not found");
  });
  server.listen(CFG.port, "127.0.0.1", () => {
    log(`面板已启动：http://127.0.0.1:${CFG.port}`);
  });
}

/* ---------------------------------------------------------------- 入口 */

(async () => {
  await fsp.mkdir(DATA, { recursive: true });
  await fsp.mkdir(RUNS, { recursive: true });
  if (process.env.NODEPUSHER_ONESHOT === "1") {
    const r = await runPipeline("oneshot");
    process.exit(r.ok ? 0 : 1);
  }
  startServer();
  scheduleNext();
  if (CFG.runOnStart) {
    log("配置了启动即运行");
    runPipeline("startup");
  }
})();
