// BoraCharts Alarm — Cloudflare Worker
// Gerekenler: KV bağlantısı adı ALARMS, gizli değişken BOT_TOKEN, Cron "* * * * *"
const H = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET,POST,DELETE,OPTIONS', 'access-control-allow-headers': 'content-type', 'content-type': 'application/json' };
const J = (d, s = 200) => new Response(JSON.stringify(d), { status: s, headers: H });
const tg = (env, method, body) => fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
const getAll = async env => JSON.parse((await env.ALARMS.get('alerts')) || '[]');
const fmt = v => Number(v).toLocaleString('tr-TR', { maximumFractionDigits: 8 });

export default {
  async fetch(req, env) {
    const url = new URL(req.url), p = url.pathname;
    if (req.method === 'OPTIONS') return new Response(null, { headers: H });

    // Bir kere aç: Telegram'ın bota mesajları buraya göndermesini sağlar
    if (p === '/setup') {
      const r = await tg(env, 'setWebhook', { url: `${url.origin}/tg`, allowed_updates: ['message'] });
      return J(await r.json());
    }

    // Telegram'dan gelen mesajlar: /start <kod> ile site ↔ sohbet eşleşir
    if (p === '/tg' && req.method === 'POST') {
      const u = await req.json().catch(() => ({}));
      const m = u.message; if (!m || !m.text) return J({ ok: true });
      const code = (m.text.match(/^\/start\s+([A-Za-z0-9_-]{8,64})/) || [])[1];
      if (code) {
        await env.ALARMS.put('link:' + code, String(m.chat.id));
        await tg(env, 'sendMessage', { chat_id: m.chat.id, text: '✅ BoraCharts bağlandı. Kurduğun alarmlar çalınca buradan haber vereceğim.' });
      } else {
        await tg(env, 'sendMessage', { chat_id: m.chat.id, text: 'Bağlanmak için BoraCharts sitesinde Kripto → Alarmlar → "Telegram\'a bağla" butonuna bas.' });
      }
      return J({ ok: true });
    }

    // Site: bağlı mı?
    if (p === '/status') {
      const code = url.searchParams.get('code') || '';
      return J({ linked: !!(code && await env.ALARMS.get('link:' + code)) });
    }

    // Site: alarm ekle / sil / listele
    if (p === '/alerts') {
      const code = url.searchParams.get('code') || '';
      if (!/^[A-Za-z0-9_-]{8,64}$/.test(code)) return J({ error: 'kod' }, 400);
      let all = await getAll(env);
      if (req.method === 'POST') {
        const b = await req.json().catch(() => ({}));
        if (!/^[A-Z0-9]{2,20}$/.test(b.sym || '') || !(b.price > 0) || !['>', '<'].includes(b.op)) return J({ error: 'veri' }, 400);
        if (all.filter(a => a.code === code).length >= 50) return J({ error: 'limit' }, 400);
        all.push({ id: String(b.id), code, sym: b.sym, price: +b.price, op: b.op });
        await env.ALARMS.put('alerts', JSON.stringify(all));
        return J({ ok: true });
      }
      if (req.method === 'DELETE') {
        const id = url.searchParams.get('id');
        all = all.filter(a => !(a.code === code && a.id === id));
        await env.ALARMS.put('alerts', JSON.stringify(all));
        return J({ ok: true });
      }
      return J(all.filter(a => a.code === code));
    }
    return J({ app: 'BoraCharts Alarm', ok: true });
  },

  // Her dakika: fiyatları kontrol et, tetiklenenleri Telegram'a gönder
  async scheduled(ev, env) {
    const all = await getAll(env);
    if (!all.length) return;
    const r = await fetch('https://data-api.binance.vision/api/v3/ticker/24hr?type=MINI');
    if (!r.ok) return;
    const px = {}; for (const t of await r.json()) px[t.symbol] = { last: +t.lastPrice, high: +t.highPrice, low: +t.lowPrice };
    const keep = [];
    for (const a of all) {
      const x = px[a.sym];
      const hit = x && (a.op === '>' ? x.last >= a.price : x.last <= a.price);
      if (!hit) { keep.push(a); continue; }
      const chat = await env.ALARMS.get('link:' + a.code);
      if (chat) await tg(env, 'sendMessage', { chat_id: chat, text: `🔔 ${a.sym} ${fmt(a.price)} seviyesine ${a.op === '>' ? 'çıktı' : 'indi'}.\nŞu an: ${fmt(x.last)}` });
    }
    if (keep.length !== all.length) await env.ALARMS.put('alerts', JSON.stringify(keep));
  }
};
