// ═══════════════════════════════════════════════════
//  Mercado Limpio — Intelligence Worker
//  Cloudflare Workers + KV
// ═══════════════════════════════════════════════════

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Auth-Token',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function checkAuth(request, env) {
  const token = request.headers.get('X-Auth-Token') || '';
  const isAdmin = token === env.ADMIN_PIN;
  const isAuth = isAdmin || token === (env.LAURA_PIN || '') || token === 'operativa';
  return { isAdmin, isAuth };
}

// ───── Helpers ─────
function today() {
  return new Date().toISOString().slice(0, 10);
}

function saldo(c) {
  const monto = Number(c.monto || 0);
  const pagado = (c.pagos || []).reduce((s, p) => s + Number(p.monto || 0), 0);
  return monto - pagado;
}

function estado(c) {
  if (saldo(c) <= 0) return 'pagado';
  if (!c.vencimiento) return 'pendiente';
  const hoy = new Date(); hoy.setHours(0, 0, 0, 0);
  const venc = new Date(c.vencimiento + 'T00:00:00');
  return venc < hoy ? 'vencido' : 'pendiente';
}

function calcDeuda(compras) {
  const hoy = new Date(); hoy.setHours(0, 0, 0, 0);
  let total = 0, vencido = 0, activas = 0, vencidas = 0;
  const porProv = {};
  for (const c of compras) {
    const s = saldo(c);
    if (s <= 0) continue;
    activas++;
    total += s;
    if (c.vencimiento) {
      const venc = new Date(c.vencimiento + 'T00:00:00');
      if (venc < hoy) { vencido += s; vencidas++; }
    }
    const prov = c.proveedor || 'Sin proveedor';
    porProv[prov] = (porProv[prov] || 0) + s;
  }
  const por_proveedor = Object.entries(porProv).sort((a, b) => b[1] - a[1]).slice(0, 10);
  return { total, vencido, activas, vencidas, por_proveedor };
}

function sumRows(rows, field) {
  return rows.reduce((s, r) => {
    const v = r[field] || r['importe_bruto'] || r['importe'] || r['total'] || 0;
    return s + Number(v);
  }, 0);
}

function calcPromedioBase(baseRows) {
  if (!baseRows || !baseRows.length) return 0;
  // Group rows by ISO week using fecha field
  const weekMap = {};
  for (const r of baseRows) {
    const f = r.fecha || r.date || '';
    const v = Number(r.importe_bruto || r.importe || r.total || 0);
    if (f) {
      const d = new Date(f);
      if (!isNaN(d)) {
        const jan1 = new Date(d.getFullYear(), 0, 1);
        const week = Math.ceil(((d - jan1) / 86400000 + jan1.getDay() + 1) / 7);
        const key = `${d.getFullYear()}-${String(week).padStart(2, '0')}`;
        weekMap[key] = (weekMap[key] || 0) + v;
      } else {
        weekMap['__no_date'] = (weekMap['__no_date'] || 0) + v;
      }
    } else {
      weekMap['__no_date'] = (weekMap['__no_date'] || 0) + v;
    }
  }
  const weeks = Object.entries(weekMap).filter(([k]) => k !== '__no_date').map(([, v]) => v);
  if (weeks.length > 0) return weeks.reduce((s, v) => s + v, 0) / weeks.length;
  // Fallback: total / estimated weeks
  const total = sumRows(baseRows, 'importe_bruto');
  return total / Math.max(1, Math.round(baseRows.length / 100));
}

async function calcKPIs(env) {
  const [compras, semRanking, semVentas, semClientes, antVentas,
         baseRanking, baseVentas, baseClientes] = await Promise.all([
    env.ML_KV.get('compras', 'json'),
    env.ML_KV.get('semana_ranking_ganancia', 'json'),
    env.ML_KV.get('semana_ventas', 'json'),
    env.ML_KV.get('semana_clientes', 'json'),
    env.ML_KV.get('ant_ventas', 'json'),
    env.ML_KV.get('base_ranking_ganancia', 'json'),
    env.ML_KV.get('base_ventas', 'json'),
    env.ML_KV.get('base_clientes', 'json'),
  ]);

  const deuda = calcDeuda(compras || []);

  // Ventas KPIs
  const semVRows = semVentas?.rows || [];
  const antVRows = antVentas?.rows || [];
  const baseVRows = baseVentas?.rows || [];
  const semActual = sumRows(semVRows, 'importe_bruto');
  const antTotal = sumRows(antVRows, 'importe_bruto');
  const promBase = calcPromedioBase(baseVRows);
  const varBase = promBase > 0 ? Math.round((semActual - promBase) / promBase * 100) : 0;
  const varAnt = antTotal > 0 ? Math.round((semActual - antTotal) / antTotal * 100) : 0;

  // Ranking productos
  const srcRanking = semRanking || baseRanking;
  const rankRows = srcRanking?.rows || [];
  const productos = rankRows.map(r => ({
    desc: r.descripcion || r.desc || r.producto || r.detalle || '',
    costo: Number(r.costo || 0),
    ventas: Number(r.ventas || r.importe || r.importe_bruto || 0),
    ganancia: Number(r.ganancia || 0),
    margen: Number(r.margen || r.margen_pct || 0),
  })).filter(p => p.desc && p.ganancia !== 0);

  const totalVentas = productos.reduce((s, p) => s + p.ventas, 0);
  const totalGanancia = productos.reduce((s, p) => s + p.ganancia, 0);
  const margenGlobal = totalVentas > 0 ? Math.round(totalGanancia / totalVentas * 100) : 0;

  // Clientes
  const cliRows = (semClientes || baseClientes)?.rows || [];
  const cliMap = {};
  for (const r of cliRows) {
    const nom = r.cliente || r.nombre || r.razon_social || '';
    const imp = Number(r.importe || r.total || 0);
    if (nom) cliMap[nom] = (cliMap[nom] || 0) + imp;
  }
  const clientesLista = Object.entries(cliMap)
    .map(([nombre, importe]) => ({ nombre, importe }))
    .sort((a, b) => b.importe - a.importe)
    .slice(0, 20);

  return {
    ventas: { semana_actual: semActual, promedio_base: promBase, var_vs_anterior: varAnt, var_vs_base: varBase },
    ranking: {
      total_ventas: totalVentas || (baseRanking ? sumRows(baseRanking.rows || [], 'ventas') : 0),
      margen_global: margenGlobal,
      cant_productos: rankRows.length,
      productos,
      source: semRanking ? 'semana' : 'base',
    },
    clientes: { lista: clientesLista },
    deuda,
    estado: {
      tiene_base: !!(baseRanking && baseVentas && baseClientes),
      tiene_semana: !!(semRanking && semVentas),
      base_completa: !!(baseRanking && baseVentas && baseClientes),
    },
  };
}

async function callGemini(env, prompt) {
  const key = env.GEMINI_API_KEY;
  if (!key) throw new Error('GEMINI_API_KEY no configurada');
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${key}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }),
  });
  const data = await res.json();
  return data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
}

async function sendEmail(env, subject, html) {
  const key = env.BREVO_API_KEY;
  if (!key) return;
  const email = (env.PABLO_EMAIL || '').trim();
  if (!email) return;
  await fetch('https://api.brevo.com/v3/smtp/email', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'api-key': key },
    body: JSON.stringify({
      sender: { name: 'Mercado Limpio', email: 'noreply@mercadolimpio.com' },
      to: [{ email }],
      subject,
      htmlContent: html,
    }),
  });
}

function buildReportPrompt(tipo, kpis, rankRows, baseRows, compras, memoria) {
  const deuda = kpis?.deuda || {};
  const ventas = kpis?.ventas || {};
  const productos = (kpis?.ranking?.productos || []).slice(0, 15);
  const insights = (memoria?.insights || []).slice(-5).join('\n- ');

  const contexto = `
NEGOCIO: Mercado Limpio (venta de artículos del hogar/bazar)
MEMORIA IA: ${insights || 'Sin insights previos'}
DEUDA PROVEEDORES: Total $${deuda.total || 0} | Vencido $${deuda.vencido || 0} | ${deuda.activas || 0} compras activas
TOP PRODUCTOS (por ganancia):
${productos.map(p => `- ${p.desc}: $${p.ganancia} ganancia, ${p.margen}% margen`).join('\n')}
`.trim();

  if (tipo === 'semanal') {
    const vSem = ventas.semana_actual || 0;
    const vBase = ventas.promedio_base || 0;
    const varB = ventas.var_vs_base || 0;
    return `Sos el analista de Mercado Limpio, un negocio de artículos del hogar/bazar en Argentina.

${contexto}

VENTAS SEMANA: $${vSem.toLocaleString('es-AR')} | Vs. base: ${varB > 0 ? '+' : ''}${varB}% | Promedio base: $${vBase.toLocaleString('es-AR')}

Generá un reporte semanal CONCISO (máx 400 palabras) con:
1. Resumen ejecutivo de la semana
2. Análisis de los productos top (qué conviene potenciar)
3. Alerta sobre la deuda de proveedores si hay riesgo
4. 2-3 recomendaciones concretas para la semana siguiente
Tono: directo, profesional, en español argentino.`;
  }

  if (tipo === 'base') {
    return `Sos el analista de Mercado Limpio.

${contexto}

BASE DEL AÑO CARGADA. Analizá la performance histórica y respondé:
1. ¿Cuáles son los 5 productos más rentables del año?
2. ¿Hay estacionalidad visible en las ventas?
3. ¿Qué productos tienen margen bajo y deberían revisarse?
4. Benchmark general del negocio.
Máx 500 palabras, tono profesional en español argentino.`;
  }

  if (tipo === 'anticipacion') {
    const mes = new Date().toLocaleString('es-AR', { month: 'long' });
    return `Sos el analista comercial de Mercado Limpio (artículos del hogar/bazar, Argentina).

${contexto}

MES ACTUAL: ${mes}

Con base en los datos del negocio y la estacionalidad típica del rubro en Argentina, generá una ANTICIPACIÓN COMERCIAL para las próximas 2-3 semanas:
1. ¿Qué categorías/productos van a tener más demanda? ¿Por qué?
2. ¿Qué conviene stockear o negociar con proveedores ahora?
3. Alertas sobre vencimientos de deuda próximos (${deuda.vencidas || 0} facturas vencidas)
4. Una acción concreta de alto impacto para esta semana.
Máx 400 palabras, tono práctico y accionable.`;
  }

  return '';
}

// ═══════════════════════════════════════════════════
//  FETCH HANDLER
// ═══════════════════════════════════════════════════
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') return new Response(null, { headers: CORS });

    const { isAdmin, isAuth } = checkAuth(request, env);
    if (!isAuth) return json({ error: 'No autorizado' }, 401);

    try {
      // ── GET /api/compras ──
      if (path === '/api/compras' && method === 'GET') {
        const data = await env.ML_KV.get('compras', 'json') || [];
        return json(data);
      }

      // ── POST /api/compras ──
      if (path === '/api/compras' && method === 'POST') {
        const body = await request.json();
        const compras = await env.ML_KV.get('compras', 'json') || [];
        const compra = {
          id: crypto.randomUUID(),
          proveedor: body.proveedor,
          monto: Number(body.monto),
          fecha: body.fecha,
          vencimiento: body.vencimiento,
          factura: body.factura || '',
          observaciones: body.observaciones || '',
          pagos: [],
        };
        compras.push(compra);
        await env.ML_KV.put('compras', JSON.stringify(compras));
        return json({ ok: true, compra });
      }

      // ── POST /api/compras/:id/pago ──
      if (path.match(/^\/api\/compras\/[^/]+\/pago$/) && method === 'POST') {
        const id = path.split('/')[3];
        const body = await request.json();
        const compras = await env.ML_KV.get('compras', 'json') || [];
        const idx = compras.findIndex(c => c.id === id);
        if (idx < 0) return json({ error: 'Compra no encontrada' }, 404);
        const pago = { ...body, id: crypto.randomUUID() };
        if (!compras[idx].pagos) compras[idx].pagos = [];
        compras[idx].pagos.push(pago);
        await env.ML_KV.put('compras', JSON.stringify(compras));
        return json({ ok: true, pago_id: pago.id });
      }

      // ── DELETE /api/compras/:id ──
      if (path.match(/^\/api\/compras\/[^/]+$/) && method === 'DELETE') {
        const id = path.split('/')[3];
        const compras = await env.ML_KV.get('compras', 'json') || [];
        const newCompras = compras.filter(c => c.id !== id);
        await env.ML_KV.put('compras', JSON.stringify(newCompras));
        return json({ ok: true });
      }

      // ── PUT /api/compras/:id  (editar monto, vencimiento, observaciones) ──
      if (path.match(/^\/api\/compras\/[^/]+$/) && method === 'PUT') {
        const id = path.split('/')[3];
        const body = await request.json();
        const compras = await env.ML_KV.get('compras', 'json') || [];
        const idx = compras.findIndex(c => c.id === id);
        if (idx < 0) return json({ error: 'Compra no encontrada' }, 404);
        if (body.monto !== undefined) compras[idx].monto = Number(body.monto);
        if (body.vencimiento !== undefined) compras[idx].vencimiento = body.vencimiento;
        if (body.observaciones !== undefined) compras[idx].observaciones = body.observaciones;
        await env.ML_KV.put('compras', JSON.stringify(compras));
        return json({ ok: true, compra: compras[idx] });
      }

      // ── GET /api/proveedores ──
      if (path === '/api/proveedores' && method === 'GET') {
        const data = await env.ML_KV.get('proveedores', 'json') || [];
        return json(data);
      }

      // ── POST /api/proveedores ──
      if (path === '/api/proveedores' && method === 'POST') {
        const body = await request.json();
        const proveedores = await env.ML_KV.get('proveedores', 'json') || [];
        const existe = proveedores.find(p => p.nombre.toLowerCase() === body.nombre.toLowerCase());
        if (existe) return json({ error: 'El proveedor ya existe' }, 400);
        const prov = { nombre: body.nombre };
        proveedores.push(prov);
        await env.ML_KV.put('proveedores', JSON.stringify(proveedores));
        return json({ ok: true, proveedor: prov });
      }

      // ── DELETE /api/proveedores/:nombre ──
      if (path.match(/^\/api\/proveedores\/.+$/) && method === 'DELETE') {
        const nombre = decodeURIComponent(path.replace('/api/proveedores/', ''));
        const proveedores = await env.ML_KV.get('proveedores', 'json') || [];
        const newProvs = proveedores.filter(p => p.nombre !== nombre);
        await env.ML_KV.put('proveedores', JSON.stringify(newProvs));
        return json({ ok: true });
      }

      // ── GET /api/kpis ──
      if (path === '/api/kpis' && method === 'GET') {
        const kpis = await calcKPIs(env);
        return json(kpis);
      }

      // ── POST /api/reporte ──
      if (path === '/api/reporte' && method === 'POST') {
        if (!isAdmin) return json({ error: 'No autorizado' }, 403);
        const { tipo } = await request.json();
        const kpis = await calcKPIs(env);
        const memoria = await env.ML_KV.get('memoria', 'json') || {};
        const prompt = buildReportPrompt(tipo, kpis, [], [], [], memoria);
        if (!prompt) return json({ error: 'Tipo inválido' }, 400);
        const reporte = await callGemini(env, prompt);

        // Save report
        const reportes = await env.ML_KV.get('reportes', 'json') || [];
        const fecha = new Date().toLocaleDateString('es-AR', { day: 'numeric', month: 'long', year: 'numeric' });
        reportes.push({ tipo, fecha, contenido: reporte });
        if (reportes.length > 20) reportes.splice(0, reportes.length - 20);
        await env.ML_KV.put('reportes', JSON.stringify(reportes));

        // Update memoria with new insights
        const insightPrompt = `De este análisis de negocio, extraé 1-2 insights clave (frases cortas, máx 30 palabras cada una) que sean útiles para futuros análisis:\n\n${reporte}`;
        const insightText = await callGemini(env, insightPrompt).catch(() => '');
        const insights = (memoria.insights || []);
        if (insightText) {
          insightText.split('\n').filter(l => l.trim().length > 10).slice(0, 2).forEach(l => {
            insights.push(l.replace(/^[-•*\d.]+\s*/, '').trim());
          });
        }
        const newMem = {
          reportes: (memoria.reportes || 0) + 1,
          ultimo_reporte: fecha,
          insights: insights.slice(-20),
        };
        await env.ML_KV.put('memoria', JSON.stringify(newMem));

        return json({ ok: true, fecha, reporte });
      }

      // ── GET /api/reportes ──
      if (path === '/api/reportes' && method === 'GET') {
        const data = await env.ML_KV.get('reportes', 'json') || [];
        return json(data);
      }

      // ── GET /api/memoria ──
      if (path === '/api/memoria' && method === 'GET') {
        const data = await env.ML_KV.get('memoria', 'json') || {};
        return json(data);
      }

      // ── POST /api/upload/:tipo ──
      if (path.match(/^\/api\/upload\/(?!base)(ranking_ganancia|ventas|clientes)$/) && method === 'POST') {
        if (!isAdmin) return json({ error: 'No autorizado' }, 403);
        const tipo = path.split('/api/upload/')[1];
        const body = await request.json();
        const rows = body.rows || [];
        // Guardar semana anterior si ya existía
        if (tipo === 'ventas') {
          const prev = await env.ML_KV.get('semana_ventas', 'json');
          if (prev) await env.ML_KV.put('ant_ventas', JSON.stringify(prev));
        }
        await env.ML_KV.put(`semana_${tipo}`, JSON.stringify({ rows, label: body.label || today() }));
        return json({ ok: true, filas: rows.length });
      }

      // ── POST /api/upload/base/:tipo ──
      if (path.match(/^\/api\/upload\/base\/(ranking_ganancia|ventas|clientes)$/) && method === 'POST') {
        if (!isAdmin) return json({ error: 'No autorizado' }, 403);
        const tipo = path.split('/api/upload/base/')[1];
        const body = await request.json();
        const rows = body.rows || [];
        await env.ML_KV.put(`base_${tipo}`, JSON.stringify({ rows, label: body.label || today() }));
        return json({ ok: true, filas: rows.length });
      }

      return json({ error: 'Ruta no encontrada' }, 404);

    } catch (err) {
      console.error(err);
      return json({ error: err.message }, 500);
    }
  },

  // ═══════════════════════════════════════════════════
  //  CRON HANDLER
  // ═══════════════════════════════════════════════════
  async scheduled(event, env, ctx) {
    const cron = event.cron;

    // "0 11 * * 1" → Lunes 11am — Reporte semanal
    // "0 13 1 * *" → 1° de cada mes 1pm — Reporte mensual
    if (cron === '0 11 * * 1' || cron === '0 13 1 * *') {
      const tipo = cron === '0 13 1 * *' ? 'semanal' : 'semanal';
      try {
        const kpis = await calcKPIs(env);
        const memoria = await env.ML_KV.get('memoria', 'json') || {};
        const prompt = buildReportPrompt(tipo, kpis, [], [], [], memoria);
        const reporte = await callGemini(env, prompt);

        const reportes = await env.ML_KV.get('reportes', 'json') || [];
        const fecha = new Date().toLocaleDateString('es-AR', { day: 'numeric', month: 'long', year: 'numeric' });
        reportes.push({ tipo, fecha, contenido: reporte });
        if (reportes.length > 20) reportes.splice(0, reportes.length - 20);
        await env.ML_KV.put('reportes', JSON.stringify(reportes));

        // Email
        const deuda = kpis.deuda || {};
        const html = `<h2>Reporte ${tipo} — ${fecha}</h2>
<p><strong>Deuda proveedores:</strong> $${(deuda.total || 0).toLocaleString('es-AR')}</p>
<p><strong>Vencido:</strong> $${(deuda.vencido || 0).toLocaleString('es-AR')}</p>
<hr>
<pre style="font-family:sans-serif;white-space:pre-wrap">${reporte}</pre>`;
        await sendEmail(env, `Mercado Limpio — Reporte ${tipo} ${fecha}`, html);
      } catch (e) {
        console.error('Cron reporte error:', e);
      }
    }

    // "0 8 * * 1-5" → Lunes-Viernes 8am — Alerta deuda vencida
    if (cron === '0 8 * * 1-5') {
      try {
        const compras = await env.ML_KV.get('compras', 'json') || [];
        const deuda = calcDeuda(compras);
        if (deuda.vencidas > 0) {
          const html = `<h2>⚠️ Alerta deuda vencida — ${today()}</h2>
<p>Hay <strong>${deuda.vencidas}</strong> facturas vencidas por un total de <strong>$${deuda.vencido.toLocaleString('es-AR')}</strong>.</p>
<h3>Por proveedor:</h3>
<ul>${deuda.por_proveedor.map(([n, v]) => `<li>${n}: $${v.toLocaleString('es-AR')}</li>`).join('')}</ul>`;
          await sendEmail(env, `⚠️ Deuda vencida: $${deuda.vencido.toLocaleString('es-AR')}`, html);
        }
      } catch (e) {
        console.error('Cron alerta error:', e);
      }
    }
  },
};
