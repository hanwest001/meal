/**
 * 구내식당 식수인원 점검 — Vercel 서버 함수 (저장소: Upstash Redis)
 *
 * 위치     : GitHub 저장소의 api/meal.js  → 사이트주소/api/meal 로 동작
 * 저장소   : Vercel [Storage]에서 Upstash for Redis를 연결하면 아래 환경변수가 자동으로 들어갑니다.
 *            KV_REST_API_URL, KV_REST_API_TOKEN  (접두어가 달라도 자동 인식)
 *            EV 충전기 현황판과 같은 저장소를 함께 써도 됩니다 — 이 서비스는 'meal:'로 시작하는 키만 사용
 * 비밀번호 : Vercel [Settings] → [Environment Variables]의 ADMIN_PW 값을 사용합니다.
 *            (저장소가 공개라서 비밀번호를 코드에 적지 않습니다)
 * 추가 설치 프로그램(package.json) 없이 작동합니다.
 */
const CONFIG = {
  MIN_COUNT: 1,        // 최소 등록 인원 (index.html과 동일하게)
  MAX_COUNT: 300,      // 입력 상한
  DEADLINE_HOUR: 9,    // 화면에 표시되는 마감 (오전 9시 30분)
  DEADLINE_MIN: 30,
  CLOSE_HOUR: 12,      // 실제 마감 (표시하지 않음) — 이 시각까지 신규 등록·수정 가능
  CLOSE_MIN: 0,
  MAX_DAYS_AHEAD: 60,  // 며칠 뒤까지 등록 허용
  MAX_FAILS: 5,        // 비밀번호 연속 오류 허용 횟수
  LOCK_MIN: 10,        // 초과 시 잠금 시간(분)
  NAME_MAX: 20,        // 소속명 최대 글자 수
  LOG_KEEP: 20000,     // 이력 보관 건수 (오래된 것부터 정리)
  DEFAULT_DEPTS: [     // 처음 한 번만 들어가는 초기 소속 (띄어쓰기 앞=버튼 윗줄)
    { id: 'A1', name: '한강서부사업단 경영보상부' },
    { id: 'A2', name: '한강서부사업단 공사관리부' },
    { id: 'A3', name: '한강서부사업단 공사1부' },
    { id: 'A4', name: '한강서부사업단 공사2부' },
    { id: 'A5', name: '한강서부사업단 공사3부' },
    { id: 'B1', name: '경기동남권지사 수지운영부' },
    { id: 'X1', name: '외부업체①(가명)' },
    { id: 'X2', name: '외부업체②(가명)' }
  ]
};

// 저장 키 (Redis)
const K_DEPTS = 'meal:depts';   // 소속 목록(JSON)
const K_REC   = 'meal:rec';     // 등록현황 — 필드 "날짜|소속ID" → {count,name,ts}
const K_LOG   = 'meal:log';     // 등록이력 — 최신순 목록
const K_FAIL  = 'meal:fail';    // 비밀번호 오류 횟수

/* ---------------- Redis 연결 (Upstash REST) ---------------- */
// 연결 시 접두어(KV_, STORAGE_ 등)가 달라도 찾을 수 있도록 '…_REST_API_URL / …_REST_API_TOKEN'을 모두 확인
function envPick(suffix, alt) {
  if (process.env['KV_' + suffix]) return process.env['KV_' + suffix];
  if (process.env[alt]) return process.env[alt];
  const k = Object.keys(process.env).find(n => n.endsWith('_' + suffix));
  return k ? process.env[k] : '';
}
const R_URL = envPick('REST_API_URL', 'UPSTASH_REDIS_REST_URL');
const R_TOKEN = envPick('REST_API_TOKEN', 'UPSTASH_REDIS_REST_TOKEN');

async function redis(cmd) {
  const r = await fetch(R_URL, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + R_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd)
  });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
async function pipeline(cmds) {
  const r = await fetch(R_URL.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + R_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  const j = await r.json();
  const bad = Array.isArray(j) ? j.find(x => x.error) : j;
  if (!Array.isArray(j) || bad) throw new Error((bad && bad.error) || 'pipeline error');
  return j.map(x => x.result);
}

/* ---------------- 요청 처리 ---------------- */
module.exports = async function handler(req, res) {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  const send = o => res.status(200).send(JSON.stringify(o));
  if (!R_URL || !R_TOKEN) return send({ ok: false, error: '저장소가 연결되지 않았습니다. Vercel [Storage]에서 Upstash Redis를 연결해 주세요.' });
  try {
    if (req.method === 'POST') {
      let b = req.body;
      if (typeof b === 'string') b = JSON.parse(b || '{}');
      b = b || {};
      if (b.action === 'admin') return send(await admin(b));
      const r = await save(b.d, b.date, b.count);
      if (r.ok) { r.depts = await getDepts(); r.days = (await summary()).days; }
      return send(r);
    }
    const p = req.query || {};
    if (p.action === 'init') { const depts = await getDepts(); return send({ ok: true, depts, days: (await summary(depts)).days }); }
    if (p.action === 'depts') return send({ ok: true, depts: await getDepts() });
    if (p.action === 'summary') return send(await summary());
    if (p.action === 'get') return send(await getOne(p.d, p.date));
    return send({ ok: false, error: '잘못된 요청입니다.' });
  } catch (e) {
    return send({ ok: false, error: '처리 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.' });
  }
};

/* ---------------- 등록 기능 ---------------- */
async function getOne(d, date) {
  const depts = await getDepts();
  if (!depts.some(x => x.id === d)) return { ok: false, error: '소속 정보가 올바르지 않습니다.' };
  if (!isDate(date)) return { ok: false, error: '날짜 형식이 올바르지 않습니다.' };
  const raw = await redis(['HGET', K_REC, date + '|' + d]);
  return { ok: true, count: raw ? (JSON.parse(raw).count || 0) : 0 };
}

async function save(d, date, count) {
  const depts = await getDepts();
  const dep = depts.find(x => x.id === d);
  if (!dep) return { ok: false, error: '소속 정보가 올바르지 않습니다. 화면을 새로 고쳐 주세요.' };
  if (!isDate(date)) return { ok: false, error: '날짜 형식이 올바르지 않습니다.' };

  const t = now();
  if (date < t.date) return { ok: false, error: '지난 날짜는 등록할 수 없습니다.' };
  if (date > addDays(t.date, CONFIG.MAX_DAYS_AHEAD)) return { ok: false, error: CONFIG.MAX_DAYS_AHEAD + '일 이내 날짜만 등록할 수 있습니다.' };
  // 실제 마감(CLOSE) 기준 — 표시 마감(9:30) 이후 ~ 실제 마감(12:00) 전에는 평소와 동일
  const late = date === t.date && t.min >= CONFIG.CLOSE_HOUR * 60 + CONFIG.CLOSE_MIN;

  count = Number(count);
  if (!Number.isInteger(count) || count < 0 || count > CONFIG.MAX_COUNT) return { ok: false, error: '인원 값이 올바르지 않습니다.' };
  if (count > 0 && count < CONFIG.MIN_COUNT) return { ok: false, error: CONFIG.MIN_COUNT + '명 이상 입력해 주세요.' };

  const field = date + '|' + d;
  const exists = !!(await redis(['HEXISTS', K_REC, field]));
  if (late && !exists) return { ok: false, error: '오늘 신규 등록은 ' + shownDeadline() + '에 마감되었습니다. 기존 등록만 수정할 수 있습니다.' };

  let kind;
  const cmds = [];
  if (count === 0) {
    if (!exists) return { ok: true, count: 0 };
    cmds.push(['HDEL', K_REC, field]); kind = '취소';
  } else {
    cmds.push(['HSET', K_REC, field, JSON.stringify({ count, name: dep.name, ts: t.stamp })]);
    kind = exists ? '수정' : '등록';
  }
  if (late) kind = '마감후' + kind;
  cmds.push(...logCmds(t.stamp, date, d, dep.name, count, kind));
  await pipeline(cmds);
  return { ok: true, count };
}

async function allRecords() {
  const h = await redis(['HGETALL', K_REC]) || [];
  const out = [];
  for (let i = 0; i < h.length; i += 2) {
    const [date, dept] = String(h[i]).split('|');
    let v; try { v = JSON.parse(h[i + 1]); } catch (e) { continue; }
    if (date && dept && v && v.count) out.push({ date, dept, saved: v.name || '', count: Number(v.count) });
  }
  return out;
}

async function summary(deptsIn) {
  const today = now().date;
  const depts = deptsIn || await getDepts();
  const names = Object.fromEntries(depts.map(x => [x.id, x.name]));
  const order = depts.map(x => x.id);
  const map = {};
  (await allRecords()).forEach(r => {
    if (r.date < today) return;
    // 소속명이 바뀌었으면 현재 이름으로, 삭제된 소속이면 등록 당시 이름으로 표시
    (map[r.date] = map[r.date] || []).push({ dept: r.dept, name: names[r.dept] || (r.saved + '(삭제됨)'), count: r.count });
  });
  const rank = id => { const i = order.indexOf(id); return i < 0 ? 999 : i; };
  const days = Object.keys(map).sort().map(date => {
    const items = map[date].sort((a, b) => rank(a.dept) - rank(b.dept));
    return { date, total: items.reduce((s, x) => s + x.count, 0), items };
  });
  return { ok: true, days };
}

/* ---------------- 관리자 기능 ---------------- */
async function admin(b) {
  const deny = await checkPw(b.pw);
  if (deny) return deny;
  if (b.op === 'login') return { ok: true, depts: await getDepts() };
  if (b.op === 'records') return records(b.from, b.to);

  const list = await getDepts();
  const t = now();

  if (b.op === 'add') {
    const name = cleanName(b.name);
    if (name.error) return name;
    if (list.some(x => x.name === name.value)) return { ok: false, error: '같은 이름의 소속이 이미 있습니다.' };
    let id;
    do { id = 'D' + Math.random().toString(36).slice(2, 7).toUpperCase(); } while (list.some(x => x.id === id));
    list.push({ id, name: name.value });
    await pipeline([['SET', K_DEPTS, JSON.stringify(list)], ...logCmds(t.stamp, '', id, name.value, '', '소속추가')]);
    return { ok: true, depts: list, id };
  }

  const idx = list.findIndex(x => x.id === String(b.id));
  if (idx < 0) return { ok: false, error: '대상 소속을 찾을 수 없습니다. 화면을 새로 고쳐 주세요.' };

  if (b.op === 'rename') {
    const name = cleanName(b.name);
    if (name.error) return name;
    if (list.some(x => x.name === name.value && x.id !== list[idx].id)) return { ok: false, error: '같은 이름의 소속이 이미 있습니다.' };
    const before = list[idx].name;
    list[idx].name = name.value;
    await pipeline([['SET', K_DEPTS, JSON.stringify(list)], ...logCmds(t.stamp, '', list[idx].id, before + ' → ' + name.value, '', '소속명변경')]);
    return { ok: true, depts: list };
  }

  if (b.op === 'delete') {
    if (list.length <= 1) return { ok: false, error: '소속은 최소 1곳 이상 있어야 합니다.' };
    const [gone] = list.splice(idx, 1);
    await pipeline([['SET', K_DEPTS, JSON.stringify(list)], ...logCmds(t.stamp, '', gone.id, gone.name, '', '소속삭제')]);
    return { ok: true, depts: list };
  }
  return { ok: false, error: '잘못된 요청입니다.' };
}

/** 관리자: 기간별 감축 기록 (지난 날짜 포함) */
async function records(from, to) {
  if (!isDate(from) || !isDate(to) || from > to) return { ok: false, error: '조회 기간이 올바르지 않습니다.' };
  const depts = await getDepts();
  const names = Object.fromEntries(depts.map(x => [x.id, x.name]));
  const rows = (await allRecords())
    .filter(r => r.date >= from && r.date <= to)
    .map(r => ({ date: r.date, dept: r.dept, name: names[r.dept] || (r.saved + '(삭제됨)'), count: r.count }))
    .sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  return { ok: true, rows, depts };
}

/** 비밀번호 확인 + 연속 오류 시 일정 시간 잠금 */
async function checkPw(pw) {
  const ADMIN_PW = process.env.ADMIN_PW;
  if (!ADMIN_PW) return { ok: false, error: '관리자 비밀번호가 설정되지 않았습니다. Vercel 환경변수 ADMIN_PW를 확인해 주세요.' };
  const fails = Number(await redis(['GET', K_FAIL]) || 0);
  if (fails >= CONFIG.MAX_FAILS) return { ok: false, error: '비밀번호를 ' + CONFIG.MAX_FAILS + '회 틀려 ' + CONFIG.LOCK_MIN + '분간 잠겼습니다.' };
  if (String(pw || '') !== String(ADMIN_PW)) {
    await pipeline([['INCR', K_FAIL], ['EXPIRE', K_FAIL, CONFIG.LOCK_MIN * 60]]);
    return { ok: false, auth: true, error: '비밀번호가 맞지 않습니다.' };
  }
  await redis(['DEL', K_FAIL]);
  return null;
}

/* ---------------- 보조 함수 ---------------- */
async function getDepts() {
  const raw = await redis(['GET', K_DEPTS]);
  if (raw) { try { const v = JSON.parse(raw); if (Array.isArray(v) && v.length) return v; } catch (e) {} }
  // 처음 실행 시 초기 소속을 저장 (이미 있으면 덮어쓰지 않음)
  await redis(['SET', K_DEPTS, JSON.stringify(CONFIG.DEFAULT_DEPTS), 'NX']);
  return JSON.parse(await redis(['GET', K_DEPTS]));
}
function logCmds(stamp, date, id, name, count, kind) {
  const line = JSON.stringify({ at: stamp, date, id, name, count, kind });
  return [['LPUSH', K_LOG, line], ['LTRIM', K_LOG, 0, CONFIG.LOG_KEEP - 1]];
}
function cleanName(v) {
  const s = String(v || '').replace(/\s+/g, ' ').trim();
  if (!s) return { ok: false, error: '소속명을 입력해 주세요.' };
  if (s.length > CONFIG.NAME_MAX) return { ok: false, error: '소속명은 ' + CONFIG.NAME_MAX + '자 이내로 입력해 주세요.' };
  return { value: s };
}
function isDate(s) { return /^\d{4}-\d{2}-\d{2}$/.test(String(s || '')); }
function now() {
  const o = {};
  new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' })
    .formatToParts(new Date()).forEach(p => { o[p.type] = p.value; });
  return { date: `${o.year}-${o.month}-${o.day}`, min: Number(o.hour) * 60 + Number(o.minute), stamp: `${o.year}-${o.month}-${o.day} ${o.hour}:${o.minute}:${o.second}` };
}
function addDays(s, n) { const [y, m, d] = s.split('-').map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); }
function shownDeadline() { return '오전 ' + CONFIG.DEADLINE_HOUR + '시' + (CONFIG.DEADLINE_MIN ? ' ' + CONFIG.DEADLINE_MIN + '분' : ''); }
