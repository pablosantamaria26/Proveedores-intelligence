// ══════════════════════════════════════════════════════════════════════════════
// MERCADO LIMPIO — Intelligence Engine v4.0
// Worker + KV + Gemini AI con memoria acumulativa
// ══════════════════════════════════════════════════════════════════════════════

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Auth-Token',
};
const json = (d, s = 200) => new Response(JSON.stringify(d), { status: s, headers: { ...CORS, 'Content-Type': 'application/json' } });
const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
const isoDate = () => new Date().toLocaleString('sv', { timeZone: 'America/Argentina/Buenos_Aires' }).slice(0, 10);
const kv = async (env, k, d) => { try { const v = await env.ML_KV.get(k); return v ? JSON.parse(v) : (d !== undefined ? d : []) } catch { return d !== undefined ? d : [] } };
const kvPut = async (env, k, v) => await env.ML_KV.put(k, JSON.stringify(v));
const parseNum = v => { if (!v && v !== 0) return 0; return parseFloat(String(v).replace(/[$\s.]/g, '').replace(',', '.').replace(/[^0-9.\-]/g, '')) || 0 };

function checkAuth(req, env, role = 'pablo') {
  const t = (req.headers.get('X-Auth-Token') || '').trim();
  const isAdmin = t === env.ADMIN_PIN || t === String(env.ADMIN_PIN);
  if (role === 'pablo') return isAdmin;
  if (role === 'any') return isAdmin || t === 'operativa' || t === (env.LAURA_PIN || '');
  return false;
}

// ══════════════════════════════════════════════════════════════════════════════
// PARSERS — Adaptados a los Excel reales de ML (2 columnas vacías A,B)
// Los headers están en row 1 columnas C-G, datos desde row 2
// El frontend manda objetos ya parseados con SheetJS, las keys son los headers normalizados
// ══════════════════════════════════════════════════════════════════════════════

function findVal(obj, ...needles) {
  if (!obj) return undefined;
  const keys = Object.keys(obj);
  for (const n of needles) {
    const found = keys.find(k => k.includes(n));
    if (found !== undefined && obj[found] !== '' && obj[found] != null) return obj[found];
  }
  return undefined;
}

function parseGanancia(rows) {
  const prods = [];
  for (const r of rows) {
    const desc = String(findVal(r, 'desc', 'producto', 'articulo') || '').trim();
    const costo = parseNum(findVal(r, 'costo', 'cost') || 0);
    const ventas = parseNum(findVal(r, 'venta', 'factur', 'ingreso') || 0);
    const ganancia = parseNum(findVal(r, 'ganan', 'utilidad', 'profit') || 0);
    if (desc && desc !== 'Descripción' && (ventas > 0 || ganancia > 0)) {
      prods.push({
        pos: prods.length + 1, desc, costo, ventas, ganancia,
        margen: ventas > 0 ? Math.round(ganancia / ventas * 1000) / 10 : 0
      });
    }
  }
  return prods.sort((a, b) => b.ganancia - a.ganancia);
}

function parseVentas(rows) {
  const facts = [];
  for (const r of rows) {
    const fechaRaw = findVal(r, 'fecha', 'date');
    const cliente = String(findVal(r, 'cliente', 'razon', 'nombre') || '').trim();
    const importe = parseNum(findVal(r, 'importe', 'bruto', 'total', 'monto') || 0);
    const tipo = String(findVal(r, 'tipo', 'comp') || '').trim();
    const numero = String(findVal(r, 'numero', 'pv', 'nro') || '').trim();
    if (!fechaRaw || importe === 0) continue;

    let fecha = '';
    const raw = String(fechaRaw).trim();
    // dd/mm/yyyy or d/m/yyyy
    const m1 = raw.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (m1) fecha = `${m1[3]}-${m1[2].padStart(2, '0')}-${m1[1].padStart(2, '0')}`;
    // yyyy-mm-dd
    else if (raw.match(/^\d{4}-\d{2}-\d{2}/)) fecha = raw.slice(0, 10);
    // Excel serial number
    else if (!isNaN(raw)) { const d = new Date(Math.round((Number(raw) - 25569) * 864e5)); fecha = d.toISOString().slice(0, 10); }
    else fecha = raw;

    if (fecha) facts.push({ fecha, cliente: cliente.split(',')[0].trim(), importe, tipo, numero });
  }
  return facts;
}

function parseClientes(rows) {
  const cls = [];
  for (const r of rows) {
    const cliente = String(findVal(r, 'cliente', 'razon', 'nombre') || '').trim();
    const cantidad = parseNum(findVal(r, 'cantidad', 'cant', 'qty') || 0);
    const importe = parseNum(findVal(r, 'importe', 'total', 'monto', 'compras') || 0);
    if (cliente && cliente !== 'Cliente' && importe > 0) {
      cls.push({ nombre: cliente, cantidad, importe });
    }
  }
  return cls.sort((a, b) => b.importe - a.importe);
}

// ══════════════════════════════════════════════════════════════════════════════
// UPLOAD HANDLER — guarda base o snapshot semanal
// ══════════════════════════════════════════════════════════════════════════════

async function handleUpload(tipo, body, env, isBase = false) {
  const { rows, label } = body;
  if (!rows?.length) return json({ error: 'Sin filas' }, 400);
  const year = String(new Date().getFullYear());
  const weekKey = isBase ? `ml:${year}_base` : `ml:${year}_sem_actual`;

  const stored = await kv(env, weekKey, {});
  let result = { ok: true, tipo, filas: rows.length };

  if (tipo === 'ranking_ganancia') {
    const prods = parseGanancia(rows);
    const totV = prods.reduce((s, p) => s + p.ventas, 0);
    const totG = prods.reduce((s, p) => s + p.ganancia, 0);
    const totC = prods.reduce((s, p) => s + p.costo, 0);
    stored.ranking = { productos: prods, total_ventas: totV, total_ganancia: totG, total_costo: totC, margen: totV > 0 ? Math.round(totG / totV * 1000) / 10 : 0, cant_productos: prods.length, fecha_carga: isoDate() };
    result = { ...result, filas: prods.length, total_ganancia: totG };
  }
  else if (tipo === 'ventas') {
    const facts = parseVentas(rows);
    const totalV = facts.reduce((s, f) => s + f.importe, 0);
    // Aggregate by client
    const byCli = {};
    facts.forEach(f => { byCli[f.cliente] = (byCli[f.cliente] || 0) + f.importe });
    const topCli = Object.entries(byCli).sort((a, b) => b[1] - a[1]).slice(0, 30).map(([n, v]) => ({ nombre: n, importe: v }));
    // Aggregate by date
    const byDate = {};
    facts.forEach(f => { byDate[f.fecha] = (byDate[f.fecha] || 0) + f.importe });
    // Date range
    const fechas = facts.map(f => f.fecha).sort();
    stored.ventas = { total: totalV, facturas: facts.length, fecha_desde: fechas[0] || '', fecha_hasta: fechas[fechas.length - 1] || '', top_clientes: topCli, por_dia: byDate, fecha_carga: isoDate() };
    result = { ...result, filas: facts.length, total_ventas: totalV };
  }
  else if (tipo === 'clientes') {
    const cls = parseClientes(rows);
    stored.clientes = { lista: cls, total_clientes: cls.length, total_facturado: cls.reduce((s, c) => s + c.importe, 0), fecha_carga: isoDate() };
    result = { ...result, filas: cls.length };
  }
  else return json({ error: 'Tipo no válido' }, 400);

  stored.ultima_actualizacion = isoDate();
  await kvPut(env, weekKey, stored);

  // If not base, also archive as weekly snapshot for history
  if (!isBase) {
    const snapKey = `ml:${year}_snap_${isoDate()}`;
    await kvPut(env, snapKey, stored);
    // Keep reference of all snapshots
    const snaps = await kv(env, `ml:${year}_snapshots`, []);
    if (!snaps.includes(isoDate())) { snaps.push(isoDate()); await kvPut(env, `ml:${year}_snapshots`, snaps); }
  }

  // Log
  const log = await kv(env, 'ml:cargas_log', []);
  log.push({ id: uid(), ...result, label, isBase, fecha: isoDate() });
  await kvPut(env, 'ml:cargas_log', log.slice(-200));

  return json(result);
}

// ══════════════════════════════════════════════════════════════════════════════
// KPIs — Dashboard data
// ══════════════════════════════════════════════════════════════════════════════

async function getKPIs(env) {
  const year = String(new Date().getFullYear());
  const [base, semActual, compras, snapsKeys] = await Promise.all([
    kv(env, `ml:${year}_base`, {}),
    kv(env, `ml:${year}_sem_actual`, {}),
    kv(env, 'ml:compras', []),
    kv(env, `ml:${year}_snapshots`, [])
  ]);

  // Previous snapshot for comparison
  let semAnterior = {};
  if (snapsKeys.length >= 2) {
    const prevKey = snapsKeys[snapsKeys.length - 2];
    semAnterior = await kv(env, `ml:${year}_snap_${prevKey}`, {});
  }

  const hoy = new Date();
  // Deuda proveedores
  const activas = compras.filter(c => {
    const pagado = (c.pagos || []).reduce((s, p) => s + Number(p.monto || 0), 0);
    return Number(c.monto || 0) - pagado > 0;
  });
  const vencidas = activas.filter(c => c.vencimiento && new Date(c.vencimiento) < hoy);
  const deudaTotal = activas.reduce((s, c) => { const p = (c.pagos || []).reduce((ss, x) => ss + Number(x.monto || 0), 0); return s + (Number(c.monto || 0) - p) }, 0);
  const deudaVenc = vencidas.reduce((s, c) => { const p = (c.pagos || []).reduce((ss, x) => ss + Number(x.monto || 0), 0); return s + (Number(c.monto || 0) - p) }, 0);

  // Deuda by proveedor
  const byProv = {};
  activas.forEach(c => { const n = c.proveedor || '?'; const p = (c.pagos || []).reduce((s, x) => s + Number(x.monto || 0), 0); byProv[n] = (byProv[n] || 0) + (Number(c.monto || 0) - p) });
  const deudaPorProv = Object.entries(byProv).sort((a, b) => b[1] - a[1]).slice(0, 8);

  // Use semana actual if available, otherwise base
  const src = semActual.ranking ? semActual : base;
  const srcLabel = semActual.ranking ? 'semana' : 'base';
  const ranking = src.ranking || {};
  const ventasData = (semActual.ventas || base.ventas || {});
  const clientesData = (semActual.clientes || base.clientes || {});

  // Base stats for comparison
  const baseVentas = base.ventas?.total || 0;
  const baseFechaDesde = base.ventas?.fecha_desde || '';
  const baseFechaHasta = base.ventas?.fecha_hasta || '';
  let semanasBase = 1;
  if (baseFechaDesde && baseFechaHasta) {
    const dias = Math.round((new Date(baseFechaHasta) - new Date(baseFechaDesde)) / 864e5);
    semanasBase = Math.max(Math.round(dias / 7), 1);
  }
  const promedioSemanalBase = Math.round(baseVentas / semanasBase);

  // Semana actual vs anterior
  const ventasSemActual = semActual.ventas?.total || 0;
  const ventasSemAnterior = semAnterior.ventas?.total || 0;
  const varVsSemAnt = ventasSemAnterior > 0 ? Math.round((ventasSemActual / ventasSemAnterior - 1) * 100) : 0;
  const varVsBase = promedioSemanalBase > 0 ? Math.round((ventasSemActual / promedioSemanalBase - 1) * 100) : 0;

  // Compras a proveedores - total comprado este año
  const comprasAnio = compras.filter(c => c.fecha?.startsWith(year));
  const totalComprado = comprasAnio.reduce((s, c) => s + Number(c.monto || 0), 0);
  const totalPagado = comprasAnio.reduce((s, c) => s + (c.pagos || []).reduce((ss, p) => ss + Number(p.monto || 0), 0), 0);

  return {
    ventas: {
      semana_actual: ventasSemActual,
      semana_anterior: ventasSemAnterior,
      var_vs_anterior: varVsSemAnt,
      promedio_base: promedioSemanalBase,
      var_vs_base: varVsBase,
      total_base: baseVentas,
      semanas_base: semanasBase,
      margen: ranking.margen || 0,
      total_facturas: ventasData.facturas || 0,
      fecha_desde: ventasData.fecha_desde || '',
      fecha_hasta: ventasData.fecha_hasta || '',
    },
    ranking: {
      source: srcLabel,
      fecha_carga: ranking.fecha_carga || '',
      productos: (ranking.productos || []).slice(0, 30),
      total_ventas: ranking.total_ventas || 0,
      total_ganancia: ranking.total_ganancia || 0,
      total_costo: ranking.total_costo || 0,
      margen_global: ranking.margen || 0,
      cant_productos: ranking.cant_productos || 0,
    },
    clientes: {
      lista: (clientesData.lista || []).slice(0, 20),
      total_clientes: clientesData.total_clientes || 0,
      total_facturado: clientesData.total_facturado || 0,
    },
    deuda: { total: deudaTotal, vencido: deudaVenc, activas: activas.length, vencidas: vencidas.length, por_proveedor: deudaPorProv },
    compras_anio: { total_comprado: totalComprado, total_pagado: totalPagado, cant_compras: comprasAnio.length },
    estado: {
      tiene_base: !!(base.ranking || base.ventas || base.clientes),
      tiene_semana: !!(semActual.ranking || semActual.ventas || semActual.clientes),
      base_completa: !!(base.ranking && base.ventas && base.clientes),
      semana_completa: !!(semActual.ranking && semActual.ventas && semActual.clientes),
      snapshots: snapsKeys.length,
    }
  };
}

// ══════════════════════════════════════════════════════════════════════════════
// GEMINI AI — Motor inteligente con memoria
// ══════════════════════════════════════════════════════════════════════════════

async function generateReport(env, tipo = 'semanal') {
  const year = String(new Date().getFullYear());
  const [kpis, memoria, snapsKeys] = await Promise.all([
    getKPIs(env),
    kv(env, 'ml:memoria_ia', { insights: [], reportes: 0 }),
    kv(env, `ml:${year}_snapshots`, [])
  ]);

  // Build history from snapshots (last 4)
  const historial = [];
  for (const sk of snapsKeys.slice(-4)) {
    const snap = await kv(env, `ml:${year}_snap_${sk}`, {});
    if (snap.ventas || snap.ranking) {
      historial.push({
        fecha: sk,
        ventas_total: snap.ventas?.total || 0,
        margen: snap.ranking?.margen || 0,
        facturas: snap.ventas?.facturas || 0,
        top3_productos: (snap.ranking?.productos || []).slice(0, 3).map(p => p.desc),
        top3_clientes: (snap.clientes?.lista || []).slice(0, 3).map(c => c.nombre),
      });
    }
  }

  const mes = new Date().getMonth() + 1;
  const mesNombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][mes - 1];

  const systemPrompt = `Sos el DIRECTOR DE INTELIGENCIA COMERCIAL de Mercado Limpio Distribuidora, una distribuidora de productos de limpieza, higiene y descartables en la zona sur del Gran Buenos Aires, Argentina.

DATOS DE LA EMPRESA:
- Dueño: Pablo Santamaría
- Equipo de ventas: Martín (principal), Ramiro (en desarrollo), Laura (admin/logística)
- Clientes: supermercados, autoservicios, almacenes
- Proveedores principales: Make/Maqe (~70% del abastecimiento), Romyl, Bio Bag, Modoplast
- Meta: crecer ventas sostenidamente, semana a semana
- Moneda: pesos argentinos (ARS)

DATOS ACTUALES (${isoDate()}):
${JSON.stringify(kpis, null, 2)}

HISTORIAL DE SEMANAS ANTERIORES:
${JSON.stringify(historial, null, 2)}

MEMORIA ACUMULADA (insights previos de la IA):
${JSON.stringify((memoria.insights || []).slice(-10), null, 2)}

ESTACIONALIDAD — MES ACTUAL: ${mesNombre} (mes ${mes})
Considerá patrones estacionales de Argentina para productos de limpieza:
- Julio/Agosto: frío, más limpieza indoor, época de gripe → más lavandina, desinfectantes
- Diciembre/Enero: fiestas, mudanzas → descartables, bolsas basura grandes
- Marzo: vuelta a clases, inicio de actividad comercial fuerte
- Invierno: menos consumo general pero más productos de limpieza pesada

REGLAS:
- Español argentino, directo, sin relleno
- Números claros con formato $X.XXX.XXX
- Siempre terminá con ACCIONES CONCRETAS numeradas
- Si detectás un insight nuevo (patrón, riesgo, oportunidad), marcálo con 💡
- Si es un dato alarmante, marcálo con ⚠️
- Cada reporte debe ser ÚNICO — no repitas lo mismo que dijiste antes
- Tené en cuenta la deuda a proveedores para evaluar la salud financiera`;

  let userPrompt = '';
  if (tipo === 'semanal') {
    userPrompt = `Generá el REPORTE SEMANAL de Mercado Limpio para esta semana.

Estructura:
1. RESUMEN EJECUTIVO (3-4 líneas con lo más importante)
2. VENTAS — análisis de la semana, comparación con anterior y con promedio base
3. PRODUCTOS — qué sube, qué baja, márgenes a vigilar
4. CLIENTES — movimientos importantes, quién compra más/menos
5. PROVEEDORES — deuda actual, vencimientos, riesgo de cash flow
6. ANTICIPACIÓN ${mesNombre.toUpperCase()} — qué productos conviene stockear, qué se viene
7. ACCIONES DE LA SEMANA — 5 acciones concretas numeradas con impacto estimado`;
  } else if (tipo === 'base') {
    userPrompt = `Generá un ANÁLISIS COMPLETO DE LA BASE ${year} de Mercado Limpio.

Esta es la primera carga del año — el acumulado de enero a la fecha. Analizá todo:
1. PANORAMA GENERAL — ventas, margen, productos, clientes
2. TOP PRODUCTOS — cuáles generan más ganancia y cuáles tienen mejor margen
3. DEPENDENCIA — concentración en proveedores y clientes, riesgos
4. OPORTUNIDADES — productos con buen margen pero pocas ventas, clientes que podrían comprar más
5. ALERTAS — márgenes negativos, productos sin movimiento, dependencias peligrosas
6. PLAN DE ACCIÓN — 5 recomendaciones estratégicas para los próximos meses`;
  } else if (tipo === 'anticipacion') {
    userPrompt = `Generá un REPORTE DE ANTICIPACIÓN para ${mesNombre} y los próximos 2 meses.

Basándote en los datos que tenés de Mercado Limpio y el conocimiento estacional del rubro de limpieza en Argentina:
1. QUÉ PRODUCTOS VAN A SUBIR DE DEMANDA y por qué
2. QUÉ CONVIENE STOCKEAR esta semana para no quedarse sin mercadería
3. QUÉ CLIENTES VAN A NECESITAR MÁS (por tipo de negocio, zona, historia)
4. OPORTUNIDADES DE PRECIO — qué negociar con proveedores ahora
5. RIESGOS — qué podría pasar si no nos anticipamos
6. PLAN DE COMPRAS SUGERIDO — lista concreta de productos a pedir extra`;
  }

  try {
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${env.GEMINI_API_KEY}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [
            { role: 'user', parts: [{ text: systemPrompt }] },
            { role: 'model', parts: [{ text: 'Entendido. Soy el Director de Inteligencia Comercial de Mercado Limpio. Tengo toda la data cargada. ¿Qué necesitás?' }] },
            { role: 'user', parts: [{ text: userPrompt }] }
          ],
          generationConfig: { maxOutputTokens: 3000, temperature: 0.4 }
        })
      }
    );

    if (!res.ok) throw new Error(`Gemini ${res.status}`);
    const d = await res.json();
    const reply = d.candidates?.[0]?.content?.parts?.[0]?.text || 'Sin respuesta de la IA';

    // Extract insights (lines with 💡)
    const newInsights = reply.split('\n').filter(l => l.includes('💡')).map(l => l.replace(/💡/g, '').trim()).filter(Boolean);

    // Save to memory
    if (!memoria.insights) memoria.insights = [];
    newInsights.forEach(i => {
      if (!memoria.insights.includes(i)) memoria.insights.push(i);
    });
    memoria.insights = memoria.insights.slice(-30); // keep last 30
    memoria.reportes = (memoria.reportes || 0) + 1;
    memoria.ultimo_reporte = isoDate();
    await kvPut(env, 'ml:memoria_ia', memoria);

    // Save report
    const reportes = await kv(env, 'ml:reportes', []);
    reportes.push({ id: uid(), tipo, fecha: isoDate(), contenido: reply });
    await kvPut(env, 'ml:reportes', reportes.slice(-20));

    return { ok: true, reporte: reply, tipo, fecha: isoDate(), insights_nuevos: newInsights.length };
  } catch (e) {
    return { ok: false, error: `Error IA: ${e.message}` };
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// EMAIL — Resend + HTML template
// ══════════════════════════════════════════════════════════════════════════════

async function sendResend(env, to, subject, html, attachments = []) {
  const key = env.RESEND_API_KEY;
  if (!key) throw new Error('RESEND_API_KEY no configurada — ejecutá: wrangler secret put RESEND_API_KEY');
  const body = {
    from: 'Administracion <administracion@mercadolimpio.ar>',
    to: Array.isArray(to) ? to : [to],
    subject,
    html,
  };
  if (attachments.length > 0) body.attachments = attachments;
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.message || `Resend error ${res.status}`);
  return data;
}

function buildProvEmailHTML(nombre, provCompras, fecha) {
  const m = v => v == null ? '$0' : '$' + Number(v).toLocaleString('es-AR', { maximumFractionDigits: 0 });
  const dAR = d => { if (!d) return '—'; const [y, mo, dd] = d.split('-'); return `${dd}/${mo}/${y}`; };
  const saldoC = c => Number(c.monto || 0) - (c.pagos || []).reduce((s, p) => s + Number(p.monto || 0), 0);
  const medioLbl = med => ({ efectivo: 'Efectivo', transferencia: 'Transferencia', cheque_propio: 'Cheque propio', cheque_terceros: 'Cheque terceros' }[med] || med || '—');
  const estC = c => {
    if (saldoC(c) <= 0) return 'pagado';
    if (!c.vencimiento) return 'pendiente';
    return new Date(c.vencimiento + 'T00:00:00') < new Date() ? 'vencido' : 'pendiente';
  };

  const tM = provCompras.reduce((s, c) => s + Number(c.monto || 0), 0);
  const tP = provCompras.reduce((s, c) => s + (c.pagos || []).reduce((ss, p) => ss + Number(p.monto || 0), 0), 0);
  const tS = tM - tP;
  const tV = provCompras.filter(c => estC(c) === 'vencido').reduce((s, c) => s + saldoC(c), 0);

  const pill = e => {
    if (e === 'pagado') return `<span style="padding:2px 9px;border-radius:20px;font-size:10px;font-weight:700;background:#dcfce7;color:#16a34a">&#10003; PAGADO</span>`;
    if (e === 'vencido') return `<span style="padding:2px 9px;border-radius:20px;font-size:10px;font-weight:700;background:#fee2e2;color:#dc2626">&#9888; VENCIDO</span>`;
    return `<span style="padding:2px 9px;border-radius:20px;font-size:10px;font-weight:700;background:#fef9c3;color:#854d0e">PENDIENTE</span>`;
  };

  const kpiTd = (label, val, color, bg, border) =>
    `<td style="padding:0 5px"><div style="background:${bg};border:1px solid ${border};border-radius:8px;padding:12px 8px;text-align:center"><div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:${color};margin-bottom:5px">${label}</div><div style="font-size:17px;font-weight:800;color:${color}">${val}</div></div></td>`;

  const compraBlocks = provCompras.map(c => {
    const s = saldoC(c), e = estC(c);
    const pagos = c.pagos || [];
    const tp = pagos.reduce((ss, p) => ss + Number(p.monto || 0), 0);
    const stripe = e === 'vencido' ? '#dc2626' : e === 'pagado' ? '#16a34a' : '#f59e0b';
    const bgRow = e === 'vencido' ? '#fff8f8' : 'white';

    const pagoRows = pagos.length
      ? `<tr><td colspan="3" style="padding:0">
          <div style="margin-left:4px;background:#f7f6f3">
            <div style="padding:5px 14px 5px 16px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#999;border-top:1px solid #eae8e4">&#128179; Pagos registrados</div>
            <table style="width:100%;border-collapse:collapse">
              ${pagos.map(p => {
                let det = medioLbl(p.medio);
                if (p.banco) det += ' — ' + p.banco;
                if (p.cheque_nro) det += ' &middot; Ch#' + p.cheque_nro;
                if (p.cheque_banco) det += ' (' + p.cheque_banco + ')';
                if (p.cheque_librador) det += ' &middot; Lib: ' + p.cheque_librador;
                if (p.cheque_fecha) det += ' &middot; Vto: ' + dAR(p.cheque_fecha);
                if (p.observaciones) det += ' &middot; ' + p.observaciones;
                return `<tr><td style="padding:6px 10px 6px 16px;font-size:11px;color:#666;border-bottom:1px solid #edecea;white-space:nowrap">${dAR(p.fecha)}</td><td style="padding:6px 10px;font-size:11px;font-weight:700;color:#16a34a;text-align:right;border-bottom:1px solid #edecea;white-space:nowrap">${m(p.monto)}</td><td style="padding:6px 14px;font-size:11px;color:#777;border-bottom:1px solid #edecea">${det}</td></tr>`;
              }).join('')}
            </table>
          </div>
        </td></tr>`
      : e !== 'pagado'
      ? `<tr><td colspan="3" style="padding:5px 14px 8px 20px;font-size:11px;color:#c0bdb8;font-style:italic;background:#faf9f6;border-top:1px solid #edecea">Sin pagos registrados aún</td></tr>`
      : '';

    return `<table style="width:100%;border-collapse:collapse;margin-bottom:10px;border:1px solid #e2e0da">
      <tr style="background:${bgRow}">
        <td style="width:4px;padding:0;background:${stripe}"></td>
        <td style="padding:11px 13px">
          <div><strong style="font-size:13px;color:#1a1a1a">${c.factura ? 'Factura #' + c.factura : 'Sin N&ordm; factura'}</strong> &nbsp;${pill(e)}</div>
          <div style="font-size:11px;color:#a0a09a;margin-top:3px">Ingreso: ${dAR(c.fecha)}${c.vencimiento ? ' &nbsp;&middot;&nbsp; Vence: ' + dAR(c.vencimiento) : ''}${c.observaciones ? ' &nbsp;&middot;&nbsp; ' + c.observaciones : ''}</div>
        </td>
        <td style="padding:11px 13px;text-align:right;white-space:nowrap;vertical-align:top">
          <div style="font-size:10px;color:#c0bdb8;margin-bottom:1px">Total &nbsp;/ &nbsp;Pagado</div>
          <div style="font-size:13px;font-weight:700;color:#2d2d2d">${m(c.monto)} &nbsp;/ &nbsp;<span style="color:#16a34a">${m(tp)}</span></div>
          <div style="font-size:14px;font-weight:900;color:${s > 0 ? '#dc2626' : '#16a34a'};margin-top:2px">Saldo: ${m(s)}</div>
        </td>
      </tr>
      ${pagoRows}
    </table>`;
  }).join('');

  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#e5e2dc;font-family:'Segoe UI',Helvetica,Arial,sans-serif">
<div style="max-width:660px;margin:0 auto;padding:20px 0 28px">

  <div style="background:linear-gradient(135deg,#0d1b2e 0%,#1a3050 50%,#166534 100%);padding:28px 30px;border-radius:10px 10px 0 0">
    <div style="font-size:9px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.38);margin-bottom:10px">Mercado Limpio &middot; Distribuidora</div>
    <div style="font-size:26px;font-weight:900;color:#fff;letter-spacing:-.7px;line-height:1.1">Estado de Cuenta</div>
    <div style="height:1px;background:rgba(255,255,255,.1);margin:10px 0"></div>
    <table style="width:100%;border-collapse:collapse"><tr>
      <td><div style="font-size:15px;font-weight:700;color:rgba(255,255,255,.88)">${nombre}</div><div style="font-size:11px;color:rgba(255,255,255,.45);margin-top:2px">Al ${fecha}</div></td>
      <td style="text-align:right;vertical-align:middle"><div style="font-size:11px;color:rgba(255,255,255,.65);background:rgba(255,255,255,.1);display:inline-block;padding:3px 12px;border-radius:20px">${provCompras.length} registro${provCompras.length !== 1 ? 's' : ''}</div></td>
    </tr></table>
  </div>

  <div style="background:#fff;padding:15px 16px;border-left:1px solid #d4d1cb;border-right:1px solid #d4d1cb;border-bottom:1px solid #e8e5df">
    <table style="width:100%;border-collapse:collapse"><tr>
      ${kpiTd('Total comprado', m(tM), '#374151', '#f9fafb', '#e5e7eb')}
      ${kpiTd('Total pagado', m(tP), '#16a34a', '#f0fdf4', '#bbf7d0')}
      ${kpiTd('Saldo pendiente', m(tS), tS > 0 ? '#b45309' : '#16a34a', tS > 0 ? '#fffbeb' : '#f0fdf4', tS > 0 ? '#fde68a' : '#bbf7d0')}
      ${tV > 0 ? kpiTd('&#9888; Vencido', m(tV), '#dc2626', '#fef2f2', '#fecaca') : kpiTd('Sin vencidos', '&#10003;', '#16a34a', '#f0fdf4', '#bbf7d0')}
    </tr></table>
  </div>

  <div style="background:#edeae4;padding:6px 16px;border-left:1px solid #d4d1cb;border-right:1px solid #d4d1cb">
    <span style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#a09d96">Detalle de compras y pagos</span>
  </div>

  <div style="background:#f5f3ef;padding:12px 12px 2px;border:1px solid #d4d1cb;border-top:none">
    ${compraBlocks}
  </div>

  <div style="background:#0d1b2e;border-radius:0 0 10px 10px;padding:15px 30px;text-align:center">
    <div style="font-size:10px;color:rgba(255,255,255,.28);margin-bottom:3px">Generado el ${new Date().toLocaleString('es-AR')}</div>
    <div style="font-size:11px;color:rgba(255,255,255,.5)">Mercado Limpio Distribuidora &nbsp;&middot;&nbsp; <a href="mailto:administracion@mercadolimpio.ar" style="color:#4ade80;text-decoration:none">administracion@mercadolimpio.ar</a></div>
  </div>

</div>
</body></html>`;
}

// ══════════════════════════════════════════════════════════════════════════════
// ROUTER
// ══════════════════════════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'OPTIONS') return new Response('', { headers: CORS });
    if (path === '/api/health') return json({ ok: true, v: '4.0' });

    // ═══ COMPRAS ═══
    if (path === '/api/compras' && request.method === 'GET') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      return json(await kv(env, 'ml:compras', []));
    }
    if (path === '/api/compras' && request.method === 'POST') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const body = await request.json();
      const compras = await kv(env, 'ml:compras', []);
      const nueva = { id: uid(), proveedor: body.proveedor, monto: Number(body.monto), factura: body.factura || '', fecha: body.fecha, vencimiento: body.vencimiento, observaciones: body.observaciones || '', pagos: [], fecha_creacion: isoDate() };
      compras.push(nueva);
      await kvPut(env, 'ml:compras', compras);
      return json({ ok: true, compra: nueva });
    }
    if (path.match(/^\/api\/compras\/[^/]+\/pago$/) && request.method === 'POST') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const id = path.split('/')[3];
      const body = await request.json();
      const compras = await kv(env, 'ml:compras', []);
      const idx = compras.findIndex(c => c.id === id);
      if (idx < 0) return json({ error: 'No encontrada' }, 404);
      const pago = { id: uid(), monto: Number(body.monto), fecha: body.fecha || isoDate(), medio: body.medio || 'efectivo', banco: body.banco || '', cheque_nro: body.cheque_nro || '', cheque_banco: body.cheque_banco || '', cheque_librador: body.cheque_librador || '', cheque_fecha: body.cheque_fecha || '', observaciones: body.observaciones || '' };
      if (!compras[idx].pagos) compras[idx].pagos = [];
      compras[idx].pagos.push(pago);
      await kvPut(env, 'ml:compras', compras);
      return json({ ok: true, pago_id: pago.id });
    }
    if (path.match(/^\/api\/compras\/[^/]+$/) && request.method === 'PUT') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const id = path.split('/')[3];
      const body = await request.json();
      const compras = await kv(env, 'ml:compras', []);
      const idx = compras.findIndex(c => c.id === id);
      if (idx < 0) return json({ error: 'No encontrada' }, 404);
      if (body.monto !== undefined) compras[idx].monto = Number(body.monto);
      if (body.vencimiento !== undefined) compras[idx].vencimiento = body.vencimiento;
      if (body.observaciones !== undefined) compras[idx].observaciones = body.observaciones;
      await kvPut(env, 'ml:compras', compras);
      return json({ ok: true, compra: compras[idx] });
    }
    if (path.match(/^\/api\/compras\/[^/]+$/) && request.method === 'DELETE') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const id = path.split('/')[3];
      const compras = await kv(env, 'ml:compras', []);
      await kvPut(env, 'ml:compras', compras.filter(c => c.id !== id));
      return json({ ok: true });
    }

    // ═══ PROVEEDORES ═══
    if (path === '/api/proveedores' && request.method === 'GET') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      return json(await kv(env, 'ml:proveedores', []));
    }
    if (path === '/api/proveedores' && request.method === 'POST') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const body = await request.json();
      if (!body.nombre) return json({ error: 'Nombre requerido' }, 400);
      const provs = await kv(env, 'ml:proveedores', []);
      if (provs.find(p => p.nombre.toLowerCase() === body.nombre.trim().toLowerCase())) return json({ error: 'Ya existe' }, 400);
      const nuevo = { id: uid(), nombre: body.nombre.trim(), fecha_alta: isoDate() };
      provs.push(nuevo);
      await kvPut(env, 'ml:proveedores', provs);
      return json({ ok: true, proveedor: nuevo });
    }
    if (path.match(/^\/api\/proveedores\/.+$/) && request.method === 'DELETE') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const nombre = decodeURIComponent(path.replace('/api/proveedores/', ''));
      const provs = await kv(env, 'ml:proveedores', []);
      await kvPut(env, 'ml:proveedores', provs.filter(p => p.nombre !== nombre));
      return json({ ok: true });
    }

    // ═══ KPIs ═══
    if (path === '/api/kpis') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      return json(await getKPIs(env));
    }

    // ═══ UPLOAD BASE ═══
    if (path.match(/^\/api\/upload\/base\//) && request.method === 'POST') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      const tipo = path.split('/')[4];
      if (!['ranking_ganancia', 'ventas', 'clientes'].includes(tipo)) return json({ error: 'Tipo no válido' }, 400);
      const body = await request.json();
      const result = await handleUpload(tipo, body, env, true);
      return result;
    }

    // ═══ UPLOAD SEMANAL ═══
    if (path.match(/^\/api\/upload\/[^/]+$/) && request.method === 'POST') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      const tipo = path.split('/')[3];
      if (!['ranking_ganancia', 'ventas', 'clientes'].includes(tipo)) return json({ error: 'Tipo no válido' }, 400);
      const body = await request.json();
      return handleUpload(tipo, body, env, false);
    }

    // ═══ REPORTES IA ═══
    if (path === '/api/reporte' && request.method === 'POST') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      const body = await request.json();
      const result = await generateReport(env, body.tipo || 'semanal');
      return json(result);
    }

    // ═══ GET REPORTES ═══
    if (path === '/api/reportes') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      return json(await kv(env, 'ml:reportes', []));
    }

    // ═══ MEMORIA IA ═══
    if (path === '/api/memoria') {
      if (!checkAuth(request, env, 'pablo')) return json({ error: 'No autorizado' }, 401);
      return json(await kv(env, 'ml:memoria_ia', { insights: [], reportes: 0 }));
    }

    // ═══ ALERTAS ═══
    if (path === '/api/alertas') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      const compras = await kv(env, 'ml:compras', []);
      const hoy = new Date();
      const alertas = [];
      compras.forEach(c => {
        if (!c.vencimiento) return;
        const pagado = (c.pagos || []).reduce((s, p) => s + Number(p.monto || 0), 0);
        const saldo = Number(c.monto || 0) - pagado;
        if (saldo <= 0) return;
        const d = Math.round((new Date(c.vencimiento) - hoy) / 864e5);
        if (d < 0) alertas.push({ gravedad: 'urgente', titulo: `VENCIDO: ${c.proveedor}`, desc: `$${saldo.toLocaleString('es-AR')} — ${Math.abs(d)}d` });
        else if (d <= 7) alertas.push({ gravedad: 'importante', titulo: `Vence ${d}d: ${c.proveedor}`, desc: `$${saldo.toLocaleString('es-AR')}` });
      });
      return json(alertas.slice(0, 10));
    }

    // ═══ EMAIL CUENTA PROVEEDOR ═══
    if (path === '/api/email/proveedor' && request.method === 'POST') {
      if (!checkAuth(request, env, 'any')) return json({ error: 'No autorizado' }, 401);
      try {
        const { nombre, to, pdf_base64 } = await request.json();
        if (!nombre || !to) return json({ error: 'Faltan parámetros: nombre y to' }, 400);
        const compras = await kv(env, 'ml:compras', []);
        const provCompras = compras.filter(c => c.proveedor === nombre).sort((a, b) => (a.fecha || '').localeCompare(b.fecha || ''));
        const fecha = new Date().toLocaleDateString('es-AR', { day: 'numeric', month: 'long', year: 'numeric' });
        const html = buildProvEmailHTML(nombre, provCompras, fecha);
        const attachments = [];
        if (pdf_base64) {
          attachments.push({
            filename: `Estado_cuenta_${nombre.replace(/\s+/g, '_')}_${isoDate()}.pdf`,
            content: pdf_base64,
          });
        }
        await sendResend(env, to, `Estado de cuenta — ${nombre} al ${fecha}`, html, attachments);
        return json({ ok: true });
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    return json({ error: 'Not found', path }, 404);
  }
};
