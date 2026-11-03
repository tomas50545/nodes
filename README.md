name: NodePusher

# 手机点一下就跑：Actions 页面 -> NodePusher -> Run workflow
on:
  workflow_dispatch:
    inputs:
      min_kbps:
        description: "最低速度 KB/s（默认 100，想要快就填 300）"
        required: false
        default: "100"
      max_speed_test:
        description: "最多测速多少个节点（默认 80，越大越慢越准）"
        required: false
        default: "80"

permissions:
  contents: write

concurrency:
  group: nodepusher
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: 检出仓库
        uses: actions/checkout@v4

      - name: 安装 Node
        uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: 安装依赖
        run: npm install js-yaml@4 --no-save --no-audit --no-fund

      - name: 下载 mihomo 内核
        run: |
          set -e
          VER=$(curl -sL https://api.github.com/repos/MetaCubeX/mihomo/releases/latest | grep -oP '"tag_name":\s*"\K[^"]+')
          echo "版本: $VER"
          for NAME in "mihomo-linux-amd64-${VER}" "mihomo-linux-amd64-compatible-${VER}"; do
            echo "尝试 $NAME"
            if curl -fsSL -o mihomo.gz "https://github.com/MetaCubeX/mihomo/releases/download/${VER}/${NAME}.gz"; then
              gunzip -f mihomo.gz && mv mihomo mihomo-bin && break
            fi
          done
          chmod +x mihomo-bin
          ./mihomo-bin -v

      - name: 抓取 + 实测 + 生成 yaml
        env:
          NODEPUSHER_CORE: ${{ github.workspace }}/mihomo-bin
          MIN_KBPS: ${{ inputs.min_kbps }}
          MAX_SPEED_TEST: ${{ inputs.max_speed_test }}
        run: |
          cat > ci-run.js <<'NODEPUSHER_SCRIPT_END'
          /**
           * NodePusher CI 版：抓取 → 去重 → TCP → 内核复测 → 测速 → 生成 clash yaml
           * 只依赖 js-yaml 和 mihomo 内核；无 HTTP 服务、无界面，跑完即退。
           *
           * 环境变量：
           *   NODEPUSHER_CORE   内核路径（默认 ./mihomo）
           *   MIN_KBPS          速度门槛，默认 100
           *   MAX_SPEED_TEST    最多测速节点数，默认 80
           *   OUT_FILE          输出文件，默认 latest.yaml
           */
          "use strict";

          const fs = require("node:fs");
          const net = require("node:net");
          const path = require("node:path");
          const { spawn } = require("node:child_process");
          const yaml = require("js-yaml");

          const CORE = process.env.NODEPUSHER_CORE || path.join(process.cwd(), "mihomo");
          const MIN_KBPS = Number(process.env.MIN_KBPS || 100);
          const MAX_SPEED = Number(process.env.MAX_SPEED_TEST || 80);
          const OUT = process.env.OUT_FILE || "latest.yaml";
          const API_PORT = 19099;
          const MIX_PORT = 18790;
          const SECRET = "ci";

          const SOURCES = [
            ["anaer/Sub", "https://raw.githubusercontent.com/anaer/Sub/main/clash.yaml"],
            ["chengaopan/AutoMergePublicNodes", "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.meta.yml"],
            ["lanzm/MetaFetch", "https://raw.githubusercontent.com/lanzm/MetaFetch/master/list.meta.yml"],
            ["sunmiao4458/free-proxy-airport", "https://raw.githubusercontent.com/sunmiao4458/free-proxy-airport/main/docs/clash.yaml"],
            ["mahdibland/V2RayAggregator", "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity.yml"],
          ];
          const MIRRORS = ["https://ghproxy.net/", "https://gh-proxy.com/"];

          const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
          const say = (m) => console.log(m);

          async function pool(items, limit, fn) {
            const out = new Array(items.length);
            let i = 0;
            await Promise.all(
              Array.from({ length: Math.min(limit, items.length) }, async () => {
                for (;;) {
                  const k = i++;
                  if (k >= items.length) return;
                  try { out[k] = await fn(items[k], k); } catch { out[k] = null; }
                }
              })
            );
            return out;
          }

          /* ------------------------------------------------ 1. 抓取 */
          async function fetchAll() {
            const docs = [];
            for (const [name, url] of SOURCES) {
              let done = false;
              for (const u of [url, ...MIRRORS.map((m) => m + url)]) {
                if (done) break;
                for (let a = 0; a < 2 && !done; a++) {
                  try {
                    const r = await fetch(u, { signal: AbortSignal.timeout(45000), headers: { "User-Agent": "NodePusher-CI" } });
                    if (!r.ok) { say(`  ${name}: HTTP ${r.status}`); break; }
                    const doc = yaml.load(await r.text());
                    const list = doc && doc.proxies;
                    if (!Array.isArray(list) || !list.length) { say(`  ${name}: 0 节点`); break; }
                    docs.push({ name, proxies: list });
                    say(`  ${name}: ${list.length}`);
                    done = true;
                  } catch (e) {
                    if (a === 1) say(`  ${name}: ${e.cause?.code || e.name}`);
                    await sleep(700);
                  }
                }
              }
            }
            if (!docs.length) throw new Error("所有订阅源都抓取失败");
            return docs;
          }

          /* ------------------------------------------------ 2. 去重 */
          function merge(docs) {
            const bad = /^(127\.|10\.|192\.168\.|0\.0\.0\.0|localhost|::1)/i;
            const seen = new Set();
            const out = [];
            for (const d of docs)
              for (const p of d.proxies) {
                if (!p || !p.server || !p.port || !p.type) continue;
                if (bad.test(String(p.server))) continue;
                if (/剩余|到期|官网|流量|过期|订阅|expire|traffic/i.test(String(p.name || ""))) continue;
                const k = `${p.type}|${p.server}|${p.port}`;
                if (seen.has(k)) continue;
                seen.add(k);
                out.push(p);
              }
            return out;
          }

          /* ------------------------------------------------ 3. TCP */
          function tcp(host, port, ms) {
            return new Promise((res) => {
              const s = new net.Socket();
              let done = false;
              const fin = (v) => { if (done) return; done = true; try { s.destroy(); } catch {} res(v); };
              s.setTimeout(ms);
              s.once("connect", () => fin(true));
              s.once("timeout", () => fin(false));
              s.once("error", () => fin(false));
              try { s.connect(port, host); } catch { fin(false); }
            });
          }

          /* ------------------------------------------------ 4/5. 内核 */
          class Core {
            constructor() { this.proc = null; }
            async start(cfgPath) {
              const out = fs.openSync("core.log", "a");
              this.proc = spawn(CORE, ["-f", cfgPath, "-d", process.cwd()], { stdio: ["ignore", out, out] });
              this.proc.on("error", (e) => say("  内核启动错误: " + e.message));
              const end = Date.now() + 30000;
              while (Date.now() < end) {
                await sleep(400);
                try {
                  const r = await fetch(`http://127.0.0.1:${API_PORT}/version`, {
                    headers: { Authorization: `Bearer ${SECRET}` }, signal: AbortSignal.timeout(1500),
                  });
                  if (r.ok || r.status === 401) return true;
                } catch {}
                if (this.proc.exitCode !== null) throw new Error("内核退出，见 core.log");
              }
              this.stop();
              throw new Error("内核启动超时");
            }
            api(p, opts = {}) {
              return fetch(`http://127.0.0.1:${API_PORT}${p}`, {
                ...opts,
                headers: { Authorization: `Bearer ${SECRET}`, "Content-Type": "application/json" },
                signal: AbortSignal.timeout(20000),
              });
            }
            async delay(name) {
              try {
                const r = await this.api(`/proxies/${encodeURIComponent(name)}/delay?timeout=5000&url=${encodeURIComponent("https://www.gstatic.com/generate_204")}`);
                if (!r.ok) return null;
                const j = await r.json();
                return typeof j.delay === "number" ? j.delay : null;
              } catch { return null; }
            }
            async mode(m) { try { await this.api("/configs", { method: "PATCH", body: JSON.stringify({ mode: m }) }); } catch {} }
            async select(n) { try { return (await this.api("/proxies/GLOBAL", { method: "PUT", body: JSON.stringify({ name: n }) })).ok; } catch { return false; } }
            download(ms = 5000) {
              return new Promise((res) => {
                const t0 = Date.now();
                let body = 0, head = false, pending = [], status = null, done = false, ttfb = null;
                const s = net.connect(MIX_PORT, "127.0.0.1");
                const fin = () => { if (done) return; done = true; clearTimeout(tm); try { s.destroy(); } catch {} res({ kbps: Math.round(body / 1024 / (Math.max(1, Date.now() - t0) / 1000)), ttfb, status, bytes: body }); };
                const tm = setTimeout(fin, ms);
                s.on("connect", () => s.write("GET http://speed.cloudflare.com/__down?bytes=20000000 HTTP/1.1\r\nHost: speed.cloudflare.com\r\nUser-Agent: NodePusher\r\nConnection: close\r\n\r\n"));
                s.on("data", (c) => {
                  if (ttfb === null) ttfb = Date.now() - t0;
                  if (head) { body += c.length; if (body > 3e6) fin(); return; }
                  pending.push(c);
                  const b = Buffer.concat(pending);
                  const i = b.indexOf("\r\n\r\n");
                  if (i >= 0) {
                    if (status === null) { const m = /^HTTP\/1\.[01] (\d{3})/.exec(b.subarray(0, 64).toString("latin1")); status = m ? +m[1] : 0; }
                    head = true; body = b.length - (i + 4); pending = [];
                  }
                });
                s.on("error", fin); s.on("close", fin);
              });
            }
            stop() {
              if (!this.proc || this.proc.exitCode !== null) return;
              try {
                if (process.platform === "win32") spawn("taskkill", ["/PID", String(this.proc.pid), "/T", "/F"], { stdio: "ignore" });
                else this.proc.kill("SIGKILL");
              } catch {}
              this.proc = null;
            }
          }

          /* ------------------------------------------------ 6. 生成 yaml */
          const FLAGS = { 香港: "🇭🇰", 台湾: "🇹🇼", 日本: "🇯🇵", 韩国: "🇰🇷", 新加坡: "🇸🇬", 美国: "🇺🇸", 英国: "🇬🇧", 德国: "🇩🇪", 法国: "🇫🇷", 荷兰: "🇳🇱", 意大利: "🇮🇹", 西班牙: "🇪🇸", 波兰: "🇵🇱", 俄罗斯: "🇷🇺", 拉脱维亚: "🇱🇻", 丹麦: "🇩🇰", 爱尔兰: "🇮🇪", 瑞士: "🇨🇭", 瑞典: "🇸🇪", 加拿大: "🇨🇦", 澳大利亚: "🇦🇺", 爱沙尼亚: "🇪🇪", 芬兰: "🇫🇮", 挪威: "🇳🇴", 捷克: "🇨🇿", 奥地利: "🇦🇹", 罗马尼亚: "🇷🇴", 葡萄牙: "🇵🇹", 比利时: "🇧🇪", 巴西: "🇧🇷", 土耳其: "🇹🇷", 印度: "🇮🇳", 中国: "🇨🇳" };
          const CODES = { HK: "香港", TW: "台湾", JP: "日本", KR: "韩国", SG: "新加坡", US: "美国", GB: "英国", UK: "英国", DE: "德国", FR: "法国", NL: "荷兰", IT: "意大利", ES: "西班牙", PL: "波兰", RU: "俄罗斯", LV: "拉脱维亚", DK: "丹麦", IE: "爱尔兰", CH: "瑞士", SE: "瑞典", CA: "加拿大", AU: "澳大利亚", EE: "爱沙尼亚", FI: "芬兰", NO: "挪威", CZ: "捷克", AT: "奥地利", RO: "罗马尼亚", PT: "葡萄牙", BE: "比利时", BR: "巴西", TR: "土耳其", IN: "印度", CN: "中国" };
          const TN = { vless: "VLESS", vmess: "VMess", trojan: "Trojan", ss: "SS", http: "HTTP", socks5: "SOCKS5", hysteria2: "HY2", hysteria: "HY", anytls: "AnyTLS", tuic: "TUIC" };

          function countryOf(n) {
            const s = String(n || "");
            for (const [cn, f] of Object.entries(FLAGS)) if (s.includes(cn)) return cn;
            for (const [cn, f] of Object.entries(FLAGS)) if (s.includes(f)) return cn;
            const m = s.toUpperCase().match(/(?:^|[^A-Z])(HK|TW|JP|KR|SG|US|GB|UK|DE|FR|NL|IT|ES|PL|RU|LV|DK|IE|CH|SE|CA|AU|EE|FI|NO|CZ|AT|RO|PT|BE|BR|TR|IN|CN)(?:[^A-Z]|$)/);
            return m && CODES[m[1]] ? CODES[m[1]] : "其它";
          }

          function build(nodes, meta) {
            const encT = new Set(["vless", "vmess", "trojan", "ss", "hysteria2", "hysteria", "anytls", "tuic", "socks5"]);
            const enc = nodes.filter((n) => encT.has(n.type)).slice(0, 15);
            const plain = nodes.filter((n) => !encT.has(n.type)).slice(0, 25);
            const chosen = [...enc, ...plain];
            const cnt = {};
            const proxies = [];
            const info = [];
            for (const n of chosen) {
              const cn = countryOf(n.name);
              const flag = FLAGS[cn] || "🌐";
              cnt[cn] = (cnt[cn] || 0) + 1;
              const mb = n._kbps / 1024;
              const sp = mb >= 0.95 ? mb.toFixed(1) + "MB/s" : Math.round(n._kbps) + "KB/s";
              const name = `${flag} ${cn} ${String(cnt[cn]).padStart(2, "0")} ${TN[n.type] || n.type} ${sp}`;
              const p = { ...n, name };
              delete p._kbps; delete p._avg; delete p._jitter;
              proxies.push(p);
              info.push({ cn, flag, type: n.type, kbps: n._kbps, name });
            }
            const names = proxies.map((p) => p.name);
            const encNames = info.filter((i) => encT.has(i.type)).map((i) => i.name);
            const plainNames = info.filter((i) => !encT.has(i.type)).map((i) => i.name);
            const regions = [...new Set(info.map((i) => i.flag))].filter((f) => info.filter((i) => i.flag === f).length >= 2);

            const groups = [
              { name: "🚀 节点选择", type: "select", proxies: ["♻️ 自动选择", "🔯 故障转移", "🎯 全球直连", ...names] },
              { name: "♻️ 自动选择", type: "url-test", url: "http://www.gstatic.com/generate_204", interval: 600, tolerance: 50, proxies: names },
              { name: "🔯 故障转移", type: "fallback", url: "http://www.gstatic.com/generate_204", interval: 600, proxies: names },
            ];
            if (encNames.length) groups.push({ name: "🔒 加密节点", type: "select", proxies: ["♻️ 自动选择", ...encNames] });
            if (plainNames.length) groups.push({ name: "⚡ 明文节点", type: "select", proxies: ["♻️ 自动选择", ...plainNames] });
            for (const f of regions) groups.push({ name: `${f} 节点`, type: "select", proxies: ["♻️ 自动选择", ...info.filter((i) => i.flag === f).map((i) => i.name)] });
            groups.push({ name: "🎯 全球直连", type: "select", proxies: ["DIRECT"] });
            groups.push({ name: "🐟 漏网之鱼", type: "select", proxies: ["🚀 节点选择", "♻️ 自动选择", "🎯 全球直连"] });

            const cnDomains = ["cn", "baidu.com", "qq.com", "tencent.com", "weixin.qq.com", "taobao.com", "tmall.com", "alicdn.com", "aliyun.com", "alipay.com", "jd.com", "360.cn", "sina.com.cn", "weibo.com", "bilibili.com", "zhihu.com", "163.com", "126.net", "netease.com", "douyin.com", "bytedance.com", "iqiyi.com", "youku.com", "csdn.net", "gitee.com", "mi.com", "xiaomi.com", "huawei.com", "honor.com", "meituan.com", "dianping.com", "ctrip.com", "12306.cn"];
            const rules = [
              "DOMAIN-SUFFIX,local,DIRECT",
              "IP-CIDR,127.0.0.0/8,DIRECT,no-resolve", "IP-CIDR,10.0.0.0/8,DIRECT,no-resolve",
              "IP-CIDR,172.16.0.0/12,DIRECT,no-resolve", "IP-CIDR,192.168.0.0/16,DIRECT,no-resolve",
              "IP-CIDR,100.64.0.0/10,DIRECT,no-resolve", "IP-CIDR,224.0.0.0/4,DIRECT,no-resolve",
              "IP-CIDR6,fe80::/10,DIRECT,no-resolve",
              ...cnDomains.map((d) => `DOMAIN-SUFFIX,${d},DIRECT`),
              "MATCH,🐟 漏网之鱼",
            ];

            const head = `# ===============================================================\n` +
              `# Clash / mihomo 免费节点（NodePusher · GitHub Actions 生成）\n` +
              `# 生成时间 : ${new Date().toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })} (UTC+8)\n` +
              `# 节点数量 : ${proxies.length} 个，全部经过真实下载测速（>=${MIN_KBPS}KB/s）\n` +
              `# 筛选链路 : 抓取 ${meta.raw} -> 去重 ${meta.dedup} -> TCP ${meta.tcp} -> 延迟 ${meta.delay} -> 测速 ${meta.tested}\n` +
              `# 安全提示 : ⚡ 明文节点不加密，敏感操作用「🔒 加密节点」\n` +
              `# ===============================================================\n`;

            const cfg = {
              "mixed-port": 7890, "allow-lan": false, mode: "rule", "log-level": "info", ipv6: false,
              "unified-delay": true, "tcp-concurrent": true,
              profile: { "store-selected": true, "store-fake-ip": true },
              dns: {
                enable: true, ipv6: false, "enhanced-mode": "fake-ip", "fake-ip-range": "198.18.0.1/16",
                "fake-ip-filter": ["*.lan", "*.local"],
                "default-nameserver": ["223.5.5.5", "119.29.29.29"],
                nameserver: ["223.5.5.5", "119.29.29.29", "https://dns.alidns.com/dns-query", "https://doh.pub/dns-query"],
              },
              proxies, "proxy-groups": groups, rules,
            };
            return head + yaml.dump(cfg, { lineWidth: -1, noRefs: true });
          }

          /* ------------------------------------------------ main */
          (async () => {
            const t0 = Date.now();
            say("① 抓取订阅源");
            const docs = await fetchAll();
            const raw = docs.reduce((a, d) => a + d.proxies.length, 0);

            say("② 去重");
            const merged = merge(docs);
            say(`   ${merged.length} 个`);
            if (!merged.length) throw new Error("去重后没有节点");

            say("③ TCP 探测");
            const aliveFlags = await pool(merged, 250, (n) => tcp(n.server, n.port, 2500));
            const alive = merged.filter((_, i) => aliveFlags[i]);
            say(`   ${alive.length}/${merged.length} 可连`);
            if (!alive.length) throw new Error("没有 TCP 可连节点");

            say("④ 生成测试配置");
            const named = alive.map((n, i) => ({ ...n, name: `${n.type}-${i}-${String(n.name || "n").slice(0, 26)}` }));
            const testCfg = {
              "mixed-port": MIX_PORT, "allow-lan": false, mode: "rule", "log-level": "warning",
              "external-controller": `127.0.0.1:${API_PORT}`, secret: SECRET,
              proxies: named,
              "proxy-groups": [{ name: "T", type: "select", proxies: named.map((n) => n.name) }],
              rules: ["MATCH,DIRECT"],
            };
            fs.writeFileSync("test-config.yaml", yaml.dump(testCfg, { lineWidth: -1, noRefs: true }));

            say("⑤ 延迟复测");
            const core = new Core();
            try {
              await core.start(path.join(process.cwd(), "test-config.yaml"));
              let live = named;
              for (let r = 1; r <= 3; r++) {
                const lat = await pool(live, 40, (n) => core.delay(n.name));
                live = live.map((n, i) => ({ ...n, [`_d${r}`]: lat[i] })).filter((n) => n[`_d${r}`] !== null);
                say(`   第 ${r} 轮：${live.length}/${named.length}`);
                if (!live.length) break;
                await sleep(1200);
              }
              if (!live.length) throw new Error("没有通过真实请求的节点");
              for (const n of live) {
                const ds = [n._d1, n._d2, n._d3].filter((x) => typeof x === "number");
                n._avg = Math.round(ds.reduce((a, b) => a + b, 0) / ds.length);
                n._jitter = Math.max(...ds) - Math.min(...ds);
              }
              live.sort((a, b) => a._avg - b._avg);

              say("⑥ 下载测速");
              await core.mode("global");
              const cand = live.slice(0, Math.min(MAX_SPEED, live.length));
              const tested = [];
              let i = 0;
              for (const n of cand) {
                if (!(await core.select(n.name))) continue;
                const d = await core.download(5000);
                i++;
                if (i % 10 === 0) say(`   ${i}/${cand.length}`);
                if (d.bytes > 80000 && (d.status === 200 || d.status === 206)) tested.push({ ...n, _kbps: d.kbps });
              }
              say(`   ${tested.length}/${cand.length} 可用`);
              const good = tested.filter((n) => n._kbps >= MIN_KBPS).sort((a, b) => b._kbps - a._kbps);
              if (!good.length) throw new Error(`没有节点达到 ${MIN_KBPS}KB/s`);

              say("⑦ 生成 yaml");
              for (const n of good) { delete n._d1; delete n._d2; delete n._d3; }
              const text = build(good, { raw, dedup: merged.length, tcp: alive.length, delay: live.length, tested: good.length });
              fs.writeFileSync(OUT, text, "utf8");
              fs.mkdirSync("runs", { recursive: true });
              const stamp = new Date();
              const fn = `runs/nodes-${stamp.toISOString().slice(0, 10).replace(/-/g, "")}-${String(stamp.getUTCHours()).padStart(2, "0")}${String(stamp.getUTCMinutes()).padStart(2, "0")}.yaml`;
              fs.writeFileSync(fn, text, "utf8");
              fs.writeFileSync("latest.json", JSON.stringify({
                count: good.length, top: good[0]._kbps, file: fn,
                at: new Date().toISOString(),
                meta: { raw, dedup: merged.length, tcp: alive.length, delay: live.length, tested: good.length },
              }, null, 2));

              // 只留最近 7 份
              const files = fs.readdirSync("runs").filter((f) => f.endsWith(".yaml")).sort().reverse();
              for (const f of files.slice(7)) fs.unlinkSync(path.join("runs", f));

              say(`✔ 完成：${good.length} 个节点（最快 ${(good[0]._kbps / 1024).toFixed(2)}MB/s），耗时 ${((Date.now() - t0) / 1000).toFixed(0)}s`);
            } finally {
              core.stop();
            }
          })().catch((e) => {
            console.error("✘ 失败：" + e.message);
            process.exit(1);
          });
          NODEPUSHER_SCRIPT_END
          node ci-run.js

      - name: 提交结果
        run: |
          git config user.name "NodePusher Bot"
          git config user.email "nodepusher@users.noreply.github.com"
          git add latest.yaml latest.json runs 2>/dev/null || true
          if git diff --cached --quiet; then
            echo "本次结果与上次相同，跳过提交"
          else
            git commit -m "更新节点配置 $(date -u +'%Y-%m-%d %H:%M UTC')"
            git push
          fi

      - name: 保存产物（方便直接在手机上点下载）
        uses: actions/upload-artifact@v4
        with:
          name: clash-yaml
          path: |
            latest.yaml
            latest.json
          retention-days: 7
