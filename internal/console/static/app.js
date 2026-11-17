// ErasureStore 控制台前端逻辑（原生 JS，无外部依赖）
let token = localStorage.getItem('console_token') || '';
let currentTab = 'buckets';
let objectToken = '';
let currentPrefix = '';
let autoTimer = null;

// ---- 基础请求 ----
async function api(path, options = {}) {
  const opts = Object.assign({ headers: { 'X-Console-Token': token } }, options);
  if (opts.body && typeof opts.body === 'object') {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(opts.body);
  }
  const resp = await fetch(path, opts);
  if (resp.status === 401) {
    showLogin();
    throw new Error('未授权');
  }
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || ('HTTP ' + resp.status));
  return data;
}

function toast(msg, ok = true) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = ok ? 'ok' : 'err';
  setTimeout(() => { el.className = ''; }, 2600);
}

function login() {
  token = document.getElementById('tokenInput').value.trim();
  localStorage.setItem('console_token', token);
  initUI();
}
function logout() {
  token = '';
  localStorage.removeItem('console_token');
  showLogin();
}
function showLogin() {
  document.getElementById('loginBox').style.display = '';
  document.getElementById('userBox').style.display = 'none';
}
function showApp() {
  document.getElementById('loginBox').style.display = 'none';
  document.getElementById('userBox').style.display = '';
  document.getElementById('tokenHint').textContent = '已登录';
}

function switchTab(name) {
  currentTab = name;
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === 'tab-' + name));
  if (name === 'buckets') loadBuckets();
  if (name === 'objects') loadObjectBuckets();
  if (name === 'disks') loadDisks();
  if (name === 'repair') loadRepairTasks();
  if (name === 'lifecycle') loadLifecycle();
}

// ---- 存储桶 ----
async function loadBuckets() {
  try {
    const rows = await api('/api/buckets');
    document.getElementById('bucketRows').innerHTML = rows.map(b => `
      <tr>
        <td><b>${esc(b.name)}</b></td>
        <td>${fmtTime(b.created_at)}</td>
        <td>${b.object_count}</td>
        <td>${fmtBytes(b.total_bytes)}</td>
        <td><button class="small ok" onclick="browseBucket('${esc(b.name)}')">浏览对象</button></td>
      </tr>`).join('') || emptyRow(5, '暂无存储桶');
  } catch (e) { toast(e.message, false); }
}

function browseBucket(name) {
  switchTab('objects');
  document.getElementById('objBucket').value = name;
  loadObjects(true);
}

// ---- 对象浏览 ----
async function loadObjectBuckets() {
  try {
    const rows = await api('/api/buckets');
    const sel = document.getElementById('objBucket');
    const prev = sel.value;
    sel.innerHTML = rows.map(b => `<option value="${esc(b.name)}">${esc(b.name)}</option>`).join('');
    if (prev) sel.value = prev;
    if (rows.length) loadObjects(true);
  } catch (e) { toast(e.message, false); }
}

async function loadObjects(reset) {
  const bucket = document.getElementById('objBucket').value;
  if (!bucket) return;
  const prefix = document.getElementById('objPrefix').value;
  if (reset) { objectToken = ''; currentPrefix = prefix; }
  try {
    const q = new URLSearchParams({ prefix: currentPrefix, delimiter: '/', max_keys: '100' });
    if (objectToken) q.set('token', objectToken);
    const res = await api(`/api/buckets/${encodeURIComponent(bucket)}/objects?` + q);
    const rows = [];
    for (const cp of res.common_prefixes || []) {
      rows.push(`<tr><td>📁 <a href="javascript:gotoPrefix('${esc(cp)}')">${esc(cp)}</a></td>
        <td colspan="4" class="muted">公共前缀</td></tr>`);
    }
    for (const e of res.entries || []) {
      rows.push(`<tr>
        <td>📄 ${esc(e.key)}</td>
        <td>${fmtBytes(e.size)}</td>
        <td>${fmtTime(e.modified_at)}</td>
        <td class="mono">${esc(e.etag.slice(0, 16))}…</td>
        <td>
          <button class="small" onclick="scanObject('${esc(bucket)}','${esc(e.key)}')">健康检查</button>
          <button class="small danger" onclick="deleteObject('${esc(bucket)}','${esc(e.key)}')">删除</button>
        </td></tr>`);
    }
    document.getElementById('objectRows').innerHTML = rows.join('') || emptyRow(5, '无对象');
    objectToken = res.next_token || '';
    document.getElementById('nextPageBtn').style.display = res.is_truncated ? '' : 'none';
    document.getElementById('pageInfo').textContent = res.is_truncated ? '还有更多数据' : '已到末尾';
  } catch (e) { toast(e.message, false); }
}

function nextPage() { loadObjects(false); }
function gotoPrefix(p) { currentPrefix = p; document.getElementById('objPrefix').value = p; objectToken = ''; loadObjects(false); }
function browseUp() {
  const p = currentPrefix.replace(/\/$/, '');
  const idx = p.lastIndexOf('/');
  currentPrefix = idx < 0 ? '' : p.slice(0, idx + 1);
  document.getElementById('objPrefix').value = currentPrefix;
  objectToken = '';
  loadObjects(false);
}

async function scanObject(bucket, key) {
  try {
    const res = await api(`/api/objects/scan?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`);
    if (res.healthy) {
      toast(`对象 ${key} 全部分片健康（${res.object.placement.length} 个分片）`);
    } else {
      toast(`对象异常：缺失 [${res.scan.missing}] 损坏 [${res.scan.corrupt}]，已触发修复`, false);
      await api('/api/repair/scan', { method: 'POST' });
    }
  } catch (e) { toast(e.message, false); }
}

async function deleteObject(bucket, key) {
  if (!confirm(`确认删除对象 ${key}？`)) return;
  try {
    await api(`/api/objects?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`, { method: 'DELETE' });
    toast('已删除');
    loadObjects(false);
  } catch (e) { toast(e.message, false); }
}

// ---- 磁盘 ----
async function loadDisks() {
  try {
    const rows = await api('/api/disks');
    document.getElementById('diskRows').innerHTML = rows.map(d => {
      const high = d.used_percent > 85;
      const cap = d.total_bytes ? Math.min(100, d.used_percent).toFixed(1) : '0.0';
      return `<tr>
        <td><b>${esc(d.id)}</b></td>
        <td>${esc(d.enclosure)}</td>
        <td class="mono">${esc(d.path)}</td>
        <td><span class="badge ${d.status}">${statusText(d.status)}</span></td>
        <td><span class="cap-bar"><span class="cap-fill ${high ? 'high' : ''}" style="width:${cap}%"></span></span>
            <span class="cap-text">${cap}%</span></td>
        <td>${fmtBytes(d.used_bytes)} / ${fmtBytes(d.total_bytes)}</td>
        <td>${d.shard_count}</td>
        <td>${d.read_errors}</td>
        <td>${d.write_errors}</td>
        <td>
          ${d.status === 'online'
            ? `<button class="small warn" onclick="setDisk('${esc(d.id)}','offline')">模拟拔盘</button>`
            : `<button class="small ok" onclick="setDisk('${esc(d.id)}','online')">插回上线</button>`}
        </td></tr>`;
    }).join('');
  } catch (e) { toast(e.message, false); }
}

async function setDisk(id, action) {
  try {
    await api(`/api/disks/${encodeURIComponent(id)}/${action}`, { method: 'POST' });
    toast(`磁盘 ${id} 已${action === 'offline' ? '离线（拔盘）' : '上线（插回）'}`);
    loadDisks();
  } catch (e) { toast(e.message, false); }
}

async function probeDisks() {
  try { await api('/api/disks/probe', { method: 'POST' }); toast('探测完成'); loadDisks(); }
  catch (e) { toast(e.message, false); }
}

function toggleAuto() {
  if (document.getElementById('autoRefresh').checked) {
    autoTimer = setInterval(() => { if (currentTab === 'disks') loadDisks(); }, 5000);
  } else {
    clearInterval(autoTimer);
  }
}

// ---- 修复任务 ----
async function loadRepairTasks() {
  try {
    const [tasks, rate] = await Promise.all([
      api('/api/repair/tasks?limit=100'), api('/api/repair/rate')]);
    document.getElementById('rateInput').value = rate.bytes_per_sec;
    document.getElementById('repairRows').innerHTML = tasks.map(t => {
      const pct = t.total_bytes ? Math.min(100, t.progress_bytes * 100 / t.total_bytes).toFixed(1) : 0;
      return `<tr>
        <td class="mono">${esc(t.id.slice(0, 24))}…</td>
        <td>${esc(t.bucket)}/${esc(t.key)}</td>
        <td>[${(t.missing_shards || []).join(',')}]</td>
        <td>[${(t.corrupt_shards || []).join(',')}]</td>
        <td><span class="badge ${t.status}">${statusText(t.status)}</span></td>
        <td><span class="progress-bar"><span class="progress-fill" style="width:${pct}%"></span></span>
            <span class="progress-text">${pct}% (${fmtBytes(t.progress_bytes)}/${fmtBytes(t.total_bytes)})</span></td>
        <td class="muted">${esc(t.error || '')}</td>
        <td>
          ${t.status === 'running' ? `<button class="small warn" onclick="pauseTask('${esc(t.id)}')">暂停</button>` : ''}
          ${t.status === 'paused' ? `<button class="small ok" onclick="resumeTask('${esc(t.id)}')">恢复</button>` : ''}
          ${(t.status === 'running' || t.status === 'paused' || t.status === 'queued')
            ? `<button class="small danger" onclick="cancelTask('${esc(t.id)}')">取消</button>` : ''}
        </td></tr>`;
    }).join('') || emptyRow(8, '暂无修复任务');
  } catch (e) { toast(e.message, false); }
}

async function setRate() {
  const v = parseInt(document.getElementById('rateInput').value || '0', 10);
  try { await api('/api/repair/rate', { method: 'PUT', body: { bytes_per_sec: v } }); toast('限速已更新'); }
  catch (e) { toast(e.message, false); }
}
async function triggerRepair() {
  try { await api('/api/repair/scan', { method: 'POST' }); toast('已触发扫描'); setTimeout(loadRepairTasks, 1500); }
  catch (e) { toast(e.message, false); }
}
async function pauseTask(id) { try { await api(`/api/repair/tasks/${encodeURIComponent(id)}/pause`, { method: 'POST' }); loadRepairTasks(); } catch (e) { toast(e.message, false); } }
async function resumeTask(id) { try { await api(`/api/repair/tasks/${encodeURIComponent(id)}/resume`, { method: 'POST' }); loadRepairTasks(); } catch (e) { toast(e.message, false); } }
async function cancelTask(id) { try { await api(`/api/repair/tasks/${encodeURIComponent(id)}/cancel`, { method: 'POST' }); loadRepairTasks(); } catch (e) { toast(e.message, false); } }

// ---- 生命周期 ----
async function loadLifecycle() {
  try {
    const [rules, events] = await Promise.all([
      api('/api/lifecycle/rules'), api('/api/lifecycle/events?limit=50')]);
    document.getElementById('lcRows').innerHTML = rules.map(r => `
      <tr>
        <td><b>${esc(r.bucket)}</b></td><td>${esc(r.prefix || '(全部)')}</td>
        <td>${r.enabled ? '✅' : '⛔'}</td>
        <td>${r.expiration_days || '—'}</td><td>${r.abort_incomplete_days || '—'}</td>
        <td>${fmtTime(r.updated_at)}</td>
        <td><button class="small danger" onclick="deleteRule('${esc(r.bucket)}')">删除</button></td>
      </tr>`).join('') || emptyRow(7, '暂无规则');
    document.getElementById('lcEventRows').innerHTML = events.map(e => `
      <tr><td>${fmtTime(e.time)}</td><td>${esc(e.bucket)}</td><td>${esc(e.key)}</td>
      <td><span class="badge running">${esc(e.action)}</span></td><td>${esc(e.reason)}</td></tr>`).join('') || emptyRow(5, '暂无执行历史');
  } catch (e) { toast(e.message, false); }
}

async function saveRule() {
  const bucket = document.getElementById('lcBucket').value.trim();
  if (!bucket) { toast('存储桶必填', false); return; }
  const body = {
    bucket,
    prefix: document.getElementById('lcPrefix').value.trim(),
    expiration_days: parseInt(document.getElementById('lcExpire').value || '0', 10),
    abort_incomplete_days: parseInt(document.getElementById('lcAbort').value || '0', 10),
    enabled: document.getElementById('lcEnabled').checked,
  };
  try {
    await api(`/api/lifecycle/rules/${encodeURIComponent(bucket)}`, { method: 'PUT', body });
    toast('规则已保存'); loadLifecycle();
  } catch (e) { toast(e.message, false); }
}
async function deleteRule(bucket) {
  if (!confirm('确认删除该生命周期规则？')) return;
  try { await api(`/api/lifecycle/rules/${encodeURIComponent(bucket)}`, { method: 'DELETE' }); loadLifecycle(); }
  catch (e) { toast(e.message, false); }
}
async function runLifecycle() {
  try { await api('/api/lifecycle/run', { method: 'POST' }); toast('生命周期任务已启动'); setTimeout(loadLifecycle, 2000); }
  catch (e) { toast(e.message, false); }
}

// ---- 工具 ----
function fmtBytes(n) {
  if (!n) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return n.toFixed(i ? 1 : 0) + ' ' + units[i];
}
function fmtTime(s) {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d)) return s;
  const p = x => String(x).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function statusText(s) {
  return { online: '在线', offline: '离线（拔盘）', faulted: '故障', queued: '排队', running: '修复中',
    paused: '已暂停', done: '完成', failed: '失败' }[s] || s;
}
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function emptyRow(n, text) { return `<tr><td colspan="${n}" class="muted" style="text-align:center;padding:24px">${text}</td></tr>`; }

// ---- 启动 ----
function initUI() {
  if (!token) { showLogin(); return; }
  api('/api/health').then(showApp).then(() => {
    switchTab('buckets');
    toggleAuto();
  }).catch(showLogin);
}
initUI();
