// Rust-DB Console — Frontend Application

(function () {
    'use strict';

    // -----------------------------------------------------------------------
    // Debug bar — always visible, so we can see handler activity even when
    // devtools aren't open and the error bar is hidden by CSS. Each call
    // prepends a timestamped line.
    // -----------------------------------------------------------------------
    const DBG_MAX = 12;
    const dbgLines = [];
    function logDbg(msg) {
        try {
            const t = new Date().toISOString().slice(11, 19);
            dbgLines.unshift(`[${t}] ${msg}`);
            if (dbgLines.length > DBG_MAX) dbgLines.length = DBG_MAX;
            const el = (typeof document !== 'undefined') ? document.getElementById('debug-bar') : null;
            if (el) el.textContent = dbgLines.join('\n');
            if (typeof console !== 'undefined') console.log('[dbg]', msg);
        } catch (_) { /* never throw from the logger */ }
    }
    logDbg('script loaded');

    // -----------------------------------------------------------------------
    // Runtime detection (Tauri desktop vs. served browser)
    // -----------------------------------------------------------------------

    // Tauri 2 exposes invoke in several places depending on version / config.
    // Try each in order so we don't silently get null.
    function resolveInvoke() {
        if (typeof window === 'undefined') return null;
        const w = window;
        if (w.__TAURI_INTERNALS__ && typeof w.__TAURI_INTERNALS__.invoke === 'function') {
            return w.__TAURI_INTERNALS__.invoke.bind(w.__TAURI_INTERNALS__);
        }
        if (w.__TAURI__ && w.__TAURI__.core && typeof w.__TAURI__.core.invoke === 'function') {
            return w.__TAURI__.core.invoke.bind(w.__TAURI__.core);
        }
        if (w.__TAURI__ && typeof w.__TAURI__.invoke === 'function') {
            return w.__TAURI__.invoke.bind(w.__TAURI__);
        }
        return null;
    }
    const tauriInvoke = resolveInvoke();
    const IS_TAURI = tauriInvoke !== null;
    // Dump what the webview actually exposes so we can diagnose Tauri globals.
    try {
        const w = window;
        const shape = {
            has___TAURI__: !!w.__TAURI__,
            has___TAURI_INTERNALS__: !!w.__TAURI_INTERNALS__,
            tauriKeys: w.__TAURI__ ? Object.keys(w.__TAURI__).slice(0, 12) : null,
            internalsKeys: w.__TAURI_INTERNALS__ ? Object.keys(w.__TAURI_INTERNALS__).slice(0, 12) : null,
        };
        logDbg('tauri env: ' + JSON.stringify(shape));
        logDbg('IS_TAURI=' + IS_TAURI + ' invokeResolved=' + (tauriInvoke !== null));
    } catch (e) {
        logDbg('tauri env probe failed: ' + (e && e.message ? e.message : e));
    }

    // UUID that works even in older WebKitGTK webviews where crypto.randomUUID
    // may be missing.
    function genId() {
        if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
            try { return crypto.randomUUID(); } catch (_) { /* fall through */ }
        }
        return (
            Date.now().toString(36) + '-' +
            Math.random().toString(36).slice(2, 10) + '-' +
            Math.random().toString(36).slice(2, 10)
        );
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    // API_BASE is '' in browser mode (same-origin) and 'http://host:port' in Tauri.
    let API_BASE = '';
    let currentConnection = null; // { id, name, host, port, ... } in Tauri mode

    // In browser mode there's only one server; keep the legacy key names.
    // In Tauri mode, namespace per-connection so each server has its own session.
    function tokenKey() { return currentConnection ? `rustdb_token_${currentConnection.id}` : 'rustdb_token'; }
    function userKey()  { return currentConnection ? `rustdb_user_${currentConnection.id}`  : 'rustdb_user'; }

    let token = IS_TAURI ? null : localStorage.getItem('rustdb_token');
    let currentUser = IS_TAURI ? '' : (localStorage.getItem('rustdb_user') || '');
    let queryHistory = JSON.parse(localStorage.getItem('rustdb_history') || '[]');
    let isSignUp = false;

    // Table detail pagination
    let tableOffset = 0;
    let currentTable = '';
    let currentTableTotal = 0;

    // -----------------------------------------------------------------------
    // Settings (persisted in localStorage)
    // -----------------------------------------------------------------------
    const SETTINGS_KEY = 'rustdb_settings';
    const SETTINGS_DEFAULTS = Object.freeze({
        showDebug: false,
        pageSize: 100,
    });

    function loadSettings() {
        try {
            const raw = localStorage.getItem(SETTINGS_KEY);
            if (!raw) return { ...SETTINGS_DEFAULTS };
            const parsed = JSON.parse(raw);
            return { ...SETTINGS_DEFAULTS, ...parsed };
        } catch {
            return { ...SETTINGS_DEFAULTS };
        }
    }

    function saveSettings() {
        try {
            localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
        } catch (_) { /* ignore */ }
    }

    function applySettings() {
        const bar = (typeof document !== 'undefined') ? document.getElementById('debug-bar') : null;
        if (bar) bar.hidden = !settings.showDebug;
    }

    const settings = loadSettings();
    // PAGE_SIZE is read from settings wherever pagination happens.
    function getPageSize() {
        const n = parseInt(settings.pageSize, 10);
        return Number.isFinite(n) && n >= 10 && n <= 1000 ? n : 100;
    }

    // -----------------------------------------------------------------------
    // API client
    // -----------------------------------------------------------------------

    async function api(method, path, body) {
        const opts = {
            method,
            headers: { 'Content-Type': 'application/json' },
        };
        if (token) {
            opts.headers['Authorization'] = 'Bearer ' + token;
        }
        if (body !== undefined) {
            opts.body = JSON.stringify(body);
        }
        const resp = await fetch(API_BASE + path, opts);
        if (resp.status === 204) return null;
        const data = resp.headers.get('content-type')?.includes('json')
            ? await resp.json()
            : null;
        if (!resp.ok) {
            throw new Error(data?.error || `HTTP ${resp.status}`);
        }
        return data;
    }

    // -----------------------------------------------------------------------
    // Auth
    // -----------------------------------------------------------------------

    async function tryAutoLogin() {
        if (!token) return false;
        try {
            const me = await api('GET', '/api/auth/me');
            currentUser = me.username;
            return true;
        } catch {
            token = null;
            localStorage.removeItem(tokenKey());
            return false;
        }
    }

    async function login(username, password) {
        const resp = await api('POST', '/api/auth/login', { username, password });
        token = resp.token;
        currentUser = resp.username;
        localStorage.setItem(tokenKey(), token);
        localStorage.setItem(userKey(), currentUser);
    }

    async function logout() {
        try { await api('POST', '/api/auth/logout'); } catch { /* ignore */ }
        token = null;
        currentUser = '';
        localStorage.removeItem(tokenKey());
        localStorage.removeItem(userKey());
    }

    async function register(username, password) {
        const resp = await api('POST', '/api/auth/register', { username, password });
        token = resp.token;
        currentUser = resp.username;
        localStorage.setItem(tokenKey(), token);
        localStorage.setItem(userKey(), currentUser);
    }

    async function checkAuthStatus() {
        try {
            const resp = await api('GET', '/api/auth/status');
            return resp.auth_enabled;
        } catch {
            return true; // assume enabled on error
        }
    }

    // -----------------------------------------------------------------------
    // DOM helpers
    // -----------------------------------------------------------------------

    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);
    const show = (el) => { el.hidden = false; };
    const hide = (el) => { el.hidden = true; };

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // -----------------------------------------------------------------------
    // Navigation
    // -----------------------------------------------------------------------

    function showScreen(name) {
        const connEl = $('#connection-screen');
        const loginEl = $('#login-screen');
        const appEl = $('#main-app');
        hide(connEl); hide(loginEl); hide(appEl);
        if (name === 'connection') show(connEl);
        else if (name === 'login')  show(loginEl);
        else                        show(appEl);
    }

    function showPage(name) {
        $$('.page').forEach(p => { p.hidden = true; });
        const page = $(`#page-${name}`);
        if (page) show(page);

        $$('.nav-link').forEach(a => a.classList.remove('active'));
        const link = $(`.nav-link[data-page="${name}"]`);
        if (link) link.classList.add('active');
    }

    function navigateTo(page) {
        showPage(page);
        if (page === 'dashboard') loadDashboard();
        if (page === 'users') loadUsers();
    }

    // -----------------------------------------------------------------------
    // Dashboard
    // -----------------------------------------------------------------------

    async function loadDashboard() {
        const grid = $('#tables-grid');
        const empty = $('#tables-empty');
        grid.innerHTML = '<div class="muted">Loading...</div>';
        hide(empty);

        try {
            const tables = await api('GET', '/api/tables');
            if (tables.length === 0) {
                grid.innerHTML = '';
                show(empty);
                return;
            }
            hide(empty);
            grid.innerHTML = tables.map(t => `
                <div class="table-card" data-table="${escapeHtml(t.name)}">
                    <div class="table-card-name">${escapeHtml(t.name)}</div>
                    <div class="table-card-meta">${t.row_count} rows &middot; ${t.columns.length} columns</div>
                    <div class="table-card-cols">
                        ${t.columns.slice(0, 6).map(c => `<span>${escapeHtml(c.name)}</span>`).join('')}
                        ${t.columns.length > 6 ? `<span>+${t.columns.length - 6} more</span>` : ''}
                    </div>
                </div>
            `).join('');
        } catch (e) {
            grid.innerHTML = `<div class="error-msg">${escapeHtml(e.message)}</div>`;
        }
    }

    // -----------------------------------------------------------------------
    // Table Detail
    // -----------------------------------------------------------------------

    async function openTable(name) {
        currentTable = name;
        tableOffset = 0;
        showPage('table');
        $('#table-name').textContent = name;

        // Load schema
        try {
            const detail = await api('GET', `/api/tables/${encodeURIComponent(name)}`);
            currentTableTotal = detail.row_count;

            $('#table-schema').innerHTML = `
                <h3>Columns</h3>
                <ul class="schema-list">
                    ${detail.columns.map(c => `
                        <li>
                            <span class="col-name">${escapeHtml(c.name)}</span>
                            <span class="col-type">${escapeHtml(c.col_type)}</span>
                            <span class="col-nullable">${c.nullable ? 'nullable' : 'NOT NULL'}</span>
                        </li>
                    `).join('')}
                </ul>
            `;

            $('#table-indexes').innerHTML = `
                <h3>Indexes</h3>
                ${detail.indexes.length === 0
                    ? '<div class="muted">No indexes</div>'
                    : `<ul class="index-list">
                        ${detail.indexes.map(idx => `
                            <li>
                                <span class="idx-name">${escapeHtml(idx.name)}</span>
                                <span class="idx-cols">(${idx.columns.map(escapeHtml).join(', ')})</span>
                                ${idx.unique ? '<span class="idx-unique">UNIQUE</span>' : ''}
                            </li>
                        `).join('')}
                    </ul>`
                }
            `;
        } catch (e) {
            $('#table-schema').innerHTML = `<div class="error-msg">${escapeHtml(e.message)}</div>`;
        }

        await loadTableData();
    }

    async function loadTableData() {
        const wrap = $('#table-data');
        wrap.innerHTML = '<div class="muted">Loading...</div>';

        try {
            const data = await api('GET', `/api/tables/${encodeURIComponent(currentTable)}/data?limit=${getPageSize()}&offset=${tableOffset}`);
            currentTableTotal = data.total;
            renderDataTable(wrap, data.columns, data.rows);
            updatePagination();
        } catch (e) {
            wrap.innerHTML = `<div class="error-msg">${escapeHtml(e.message)}</div>`;
        }
    }

    function updatePagination() {
        const pageSize = getPageSize();
        const end = Math.min(tableOffset + pageSize, currentTableTotal);
        $('#data-info').textContent = currentTableTotal === 0
            ? 'No rows'
            : `Showing ${tableOffset + 1}-${end} of ${currentTableTotal}`;
        $('#data-prev').disabled = tableOffset === 0;
        $('#data-next').disabled = tableOffset + pageSize >= currentTableTotal;
    }

    // -----------------------------------------------------------------------
    // Data table renderer
    // -----------------------------------------------------------------------

    function renderDataTable(container, columns, rows) {
        if (!columns || columns.length === 0 || rows.length === 0) {
            container.innerHTML = '<div class="muted">No data</div>';
            return;
        }

        const thead = columns.map(c => `<th>${escapeHtml(c)}</th>`).join('');
        const tbody = rows.map(row => {
            const cells = columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) {
                    return `<td class="null-val">NULL</td>`;
                }
                return `<td>${escapeHtml(String(val))}</td>`;
            }).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        container.innerHTML = `
            <table class="data-table">
                <thead><tr>${thead}</tr></thead>
                <tbody>${tbody}</tbody>
            </table>
        `;
    }

    // -----------------------------------------------------------------------
    // SQL Console
    // -----------------------------------------------------------------------

    // Most recent successful SELECT result, used by the export buttons.
    let lastQueryResult = null;

    async function executeQuery() {
        const sql = $('#sql-input').value.trim();
        if (!sql) return;

        hide($('#query-error'));
        hide($('#query-message'));
        hide($('#export-group'));
        lastQueryResult = null;
        $('#query-result').innerHTML = '';
        $('#query-status').textContent = 'Executing...';
        $('#run-query').disabled = true;

        const startTime = performance.now();

        try {
            const result = await api('POST', '/api/query', { sql });
            const elapsed = (performance.now() - startTime).toFixed(0);

            // Add to history
            addToHistory(sql);

            if (result.type === 'query' && result.rows) {
                renderDataTable($('#query-result'), result.columns, result.rows);
                $('#query-status').textContent = `${result.rows.length} row(s) in ${elapsed}ms`;
                lastQueryResult = { columns: result.columns, rows: result.rows };
                show($('#export-group'));
            } else if (result.message) {
                const msg = $('#query-message');
                msg.textContent = result.message;
                show(msg);
                $('#query-status').textContent = `Done in ${elapsed}ms`;
            } else {
                $('#query-status').textContent = `Done in ${elapsed}ms`;
            }
        } catch (e) {
            const err = $('#query-error');
            err.textContent = e.message;
            show(err);
            $('#query-status').textContent = 'Error';
        } finally {
            $('#run-query').disabled = false;
        }
    }

    // -----------------------------------------------------------------------
    // Result export (CSV / JSON)
    // -----------------------------------------------------------------------

    function csvEscape(val) {
        if (val === null || val === undefined) return '';
        const s = String(val);
        if (/[",\n\r]/.test(s)) {
            return '"' + s.replace(/"/g, '""') + '"';
        }
        return s;
    }

    function resultAsCsv(result) {
        const { columns, rows } = result;
        const lines = [columns.map(csvEscape).join(',')];
        for (const row of rows) {
            lines.push(columns.map(c => csvEscape(row[c])).join(','));
        }
        return lines.join('\n') + '\n';
    }

    function resultAsJson(result) {
        return JSON.stringify(result.rows, null, 2);
    }

    function downloadBlob(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        // Defer revoke so the webview has a chance to start the download.
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }

    function exportResult(kind) {
        if (!lastQueryResult) return;
        const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        if (kind === 'csv') {
            downloadBlob(resultAsCsv(lastQueryResult), `query_${stamp}.csv`, 'text/csv;charset=utf-8');
        } else {
            downloadBlob(resultAsJson(lastQueryResult), `query_${stamp}.json`, 'application/json;charset=utf-8');
        }
    }

    function addToHistory(sql) {
        // Remove duplicate if present
        queryHistory = queryHistory.filter(h => h !== sql);
        queryHistory.unshift(sql);
        if (queryHistory.length > 50) queryHistory = queryHistory.slice(0, 50);
        localStorage.setItem('rustdb_history', JSON.stringify(queryHistory));
        renderHistory();
    }

    function renderHistory() {
        const list = $('#query-history');
        list.innerHTML = queryHistory.map((sql, i) =>
            `<li data-index="${i}" title="${escapeHtml(sql)}">${escapeHtml(sql)}</li>`
        ).join('');
    }

    // -----------------------------------------------------------------------
    // Query tabs — multiple open SQL buffers, persisted in localStorage
    // -----------------------------------------------------------------------

    const TABS_KEY = 'rustdb_tabs';
    let queryTabs = [];      // [{ id, sql }]
    let activeTabId = null;

    function loadTabs() {
        try {
            const raw = localStorage.getItem(TABS_KEY);
            if (raw) {
                const parsed = JSON.parse(raw);
                if (Array.isArray(parsed.tabs)) queryTabs = parsed.tabs;
                if (typeof parsed.activeId === 'string') activeTabId = parsed.activeId;
            }
        } catch { /* ignore */ }
        if (queryTabs.length === 0) {
            queryTabs.push({ id: genId(), sql: '' });
        }
        if (!activeTabId || !queryTabs.find(t => t.id === activeTabId)) {
            activeTabId = queryTabs[0].id;
        }
    }

    function saveTabs() {
        try {
            localStorage.setItem(TABS_KEY, JSON.stringify({
                tabs: queryTabs,
                activeId: activeTabId,
            }));
        } catch { /* ignore */ }
    }

    function tabLabel(tab, index) {
        const trimmed = (tab.sql || '').trim();
        if (!trimmed) return `Query ${index + 1}`;
        const firstLine = trimmed.split('\n')[0];
        return firstLine.length > 28 ? firstLine.slice(0, 28) + '…' : firstLine;
    }

    function renderTabs() {
        const container = $('#query-tabs-list');
        if (!container) return;
        container.innerHTML = queryTabs.map((t, i) => `
            <div class="query-tab ${t.id === activeTabId ? 'active' : ''}" data-tab="${escapeHtml(t.id)}" title="${escapeHtml(tabLabel(t, i))}">
                <span class="tab-title">${escapeHtml(tabLabel(t, i))}</span>
                <button class="tab-close" data-close="${escapeHtml(t.id)}" title="Close tab">&times;</button>
            </div>
        `).join('');
    }

    // Save the current textarea into the active tab's sql field (without
    // refreshing the UI — used in places where we're about to switch tabs).
    function flushActiveTab() {
        const tab = queryTabs.find(t => t.id === activeTabId);
        if (tab) tab.sql = $('#sql-input').value;
    }

    function switchToTab(id) {
        if (id === activeTabId) return;
        flushActiveTab();
        activeTabId = id;
        const tab = queryTabs.find(t => t.id === id);
        if (tab) $('#sql-input').value = tab.sql;
        // Clear the result area when switching tabs — results belong to a
        // specific query, not the editor buffer.
        $('#query-result').innerHTML = '';
        hide($('#query-error'));
        hide($('#query-message'));
        hide($('#export-group'));
        lastQueryResult = null;
        $('#query-status').textContent = '';
        saveTabs();
        renderTabs();
    }

    function newTab() {
        flushActiveTab();
        const tab = { id: genId(), sql: '' };
        queryTabs.push(tab);
        activeTabId = tab.id;
        $('#sql-input').value = '';
        $('#query-result').innerHTML = '';
        hide($('#query-error'));
        hide($('#query-message'));
        hide($('#export-group'));
        lastQueryResult = null;
        $('#query-status').textContent = '';
        saveTabs();
        renderTabs();
        $('#sql-input').focus();
    }

    function closeTab(id) {
        const idx = queryTabs.findIndex(t => t.id === id);
        if (idx === -1) return;
        if (queryTabs.length === 1) {
            // Never leave the user with zero tabs — clear the last one instead.
            queryTabs[0].sql = '';
            $('#sql-input').value = '';
            saveTabs();
            renderTabs();
            return;
        }
        queryTabs.splice(idx, 1);
        if (activeTabId === id) {
            activeTabId = queryTabs[Math.min(idx, queryTabs.length - 1)].id;
            const newActive = queryTabs.find(t => t.id === activeTabId);
            if (newActive) $('#sql-input').value = newActive.sql;
        }
        saveTabs();
        renderTabs();
    }

    // -----------------------------------------------------------------------
    // Users
    // -----------------------------------------------------------------------

    async function loadUsers() {
        try {
            const me = await api('GET', '/api/auth/me');
            if (!me.auth_enabled) {
                show($('#users-disabled'));
                hide($('#users-enabled'));
                return;
            }
            hide($('#users-disabled'));
            show($('#users-enabled'));

            const users = await api('GET', '/api/users');
            const tbody = $('#users-list');
            tbody.innerHTML = users.map(u => `
                <tr>
                    <td>${escapeHtml(u.username)}</td>
                    <td>
                        <button class="btn btn-danger btn-sm delete-user-btn" data-user="${escapeHtml(u.username)}">
                            Delete
                        </button>
                    </td>
                </tr>
            `).join('');
        } catch (e) {
            const err = $('#user-error');
            err.textContent = e.message;
            show(err);
        }
    }

    async function addUser() {
        const nameInput = $('#new-user-name');
        const passInput = $('#new-user-pass');
        const username = nameInput.value.trim();
        const password = passInput.value;
        hide($('#user-error'));

        if (!username || !password) return;

        try {
            await api('POST', '/api/users', { username, password });
            nameInput.value = '';
            passInput.value = '';
            await loadUsers();
        } catch (e) {
            const err = $('#user-error');
            err.textContent = e.message;
            show(err);
        }
    }

    async function deleteUser(username) {
        if (!confirm(`Delete user "${username}"?`)) return;
        hide($('#user-error'));
        try {
            await api('DELETE', `/api/users/${encodeURIComponent(username)}`);
            await loadUsers();
        } catch (e) {
            const err = $('#user-error');
            err.textContent = e.message;
            show(err);
        }
    }

    // -----------------------------------------------------------------------
    // Event bindings
    // -----------------------------------------------------------------------

    function bindEvents() {
        // Login / Register form
        $('#login-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            hide($('#login-error'));
            try {
                if (isSignUp) {
                    await register($('#login-user').value, $('#login-pass').value);
                } else {
                    await login($('#login-user').value, $('#login-pass').value);
                }
                showApp();
            } catch (err) {
                const el = $('#login-error');
                el.textContent = err.message;
                show(el);
            }
        });

        // Toggle between Sign In / Sign Up
        $('#auth-toggle-link').addEventListener('click', (e) => {
            e.preventDefault();
            isSignUp = !isSignUp;
            hide($('#login-error'));
            if (isSignUp) {
                $('#auth-submit').textContent = 'Sign Up';
                $('#auth-toggle-text').textContent = 'Already have an account?';
                $('#auth-toggle-link').textContent = 'Sign In';
            } else {
                $('#auth-submit').textContent = 'Sign In';
                $('#auth-toggle-text').textContent = "Don't have an account?";
                $('#auth-toggle-link').textContent = 'Sign Up';
            }
        });

        // Logout
        $('#logout-btn').addEventListener('click', async () => {
            await logout();
            if (IS_TAURI) {
                await openConnectionPicker();
            } else {
                showScreen('login');
            }
        });

        // Switch connection (Tauri only — button is hidden in browser mode)
        const switchBtn = $('#switch-conn-btn');
        if (switchBtn) {
            switchBtn.addEventListener('click', async () => {
                // Keep the token in localStorage for next time we pick this conn,
                // but drop the in-memory session and return to the picker.
                token = null;
                currentUser = '';
                await openConnectionPicker();
            });
        }

        // Navigation
        $$('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                navigateTo(link.dataset.page);
            });
        });

        // Dashboard table cards (delegated)
        $('#tables-grid').addEventListener('click', (e) => {
            const card = e.target.closest('.table-card');
            if (card) openTable(card.dataset.table);
        });

        // Refresh tables
        $('#refresh-tables').addEventListener('click', loadDashboard);

        // Back to dashboard
        $('#back-to-dashboard').addEventListener('click', () => navigateTo('dashboard'));

        // Table data pagination
        $('#data-prev').addEventListener('click', () => {
            tableOffset = Math.max(0, tableOffset - getPageSize());
            loadTableData();
        });
        $('#data-next').addEventListener('click', () => {
            tableOffset += getPageSize();
            loadTableData();
        });

        // SQL Console
        $('#run-query').addEventListener('click', executeQuery);
        $('#clear-query').addEventListener('click', () => {
            $('#sql-input').value = '';
            flushActiveTab();
            saveTabs();
            renderTabs();
            hide($('#query-error'));
            hide($('#query-message'));
            hide($('#export-group'));
            lastQueryResult = null;
            $('#query-result').innerHTML = '';
            $('#query-status').textContent = '';
        });

        // Ctrl+Enter / Cmd+Enter to execute
        $('#sql-input').addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                e.preventDefault();
                executeQuery();
            }
        });

        // Persist editor changes into the active tab and re-title it.
        $('#sql-input').addEventListener('input', () => {
            const tab = queryTabs.find(t => t.id === activeTabId);
            if (tab) {
                tab.sql = $('#sql-input').value;
                saveTabs();
                renderTabs();
            }
        });

        // Tab bar — delegated click (switch vs close)
        $('#query-tabs-list').addEventListener('click', (e) => {
            const closeBtn = e.target.closest('.tab-close');
            if (closeBtn) {
                e.stopPropagation();
                closeTab(closeBtn.dataset.close);
                return;
            }
            const tabEl = e.target.closest('.query-tab');
            if (tabEl) switchToTab(tabEl.dataset.tab);
        });

        // New tab button
        $('#new-tab-btn').addEventListener('click', newTab);

        // Export buttons
        $('#export-csv').addEventListener('click', () => exportResult('csv'));
        $('#export-json').addEventListener('click', () => exportResult('json'));

        // Query history click — load into active tab
        $('#query-history').addEventListener('click', (e) => {
            const li = e.target.closest('li');
            if (li) {
                const idx = parseInt(li.dataset.index);
                if (queryHistory[idx]) {
                    $('#sql-input').value = queryHistory[idx];
                    const tab = queryTabs.find(t => t.id === activeTabId);
                    if (tab) {
                        tab.sql = queryHistory[idx];
                        saveTabs();
                        renderTabs();
                    }
                }
            }
        });

        // Add user
        $('#add-user-btn').addEventListener('click', addUser);
        $('#new-user-pass').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') addUser();
        });

        // Delete user (delegated)
        $('#users-list').addEventListener('click', (e) => {
            const btn = e.target.closest('.delete-user-btn');
            if (btn) deleteUser(btn.dataset.user);
        });
    }

    // -----------------------------------------------------------------------
    // Connection picker (Tauri only)
    // -----------------------------------------------------------------------

    function showConnForm(initial) {
        hide($('#conn-list-view'));
        show($('#conn-form'));
        hide($('#conn-error'));
        $('#conn-name').value = initial?.name || '';
        $('#conn-host').value = initial?.host || '127.0.0.1';
        $('#conn-port').value = initial?.port || '8080';
        $('#conn-name').focus();
    }

    function showConnList() {
        show($('#conn-list-view'));
        hide($('#conn-form'));
    }

    async function renderConnList() {
        const listEl = $('#conn-list');
        const emptyEl = $('#conn-empty');
        let conns;
        try {
            conns = await tauriInvoke('list_connections');
        } catch (err) {
            showConnError('Could not load connections: ' + (err && err.message ? err.message : String(err)));
            conns = [];
        }
        if (!conns || conns.length === 0) {
            listEl.innerHTML = '';
            show(emptyEl);
            return;
        }
        hide(emptyEl);
        listEl.innerHTML = conns.map(c => `
            <li class="conn-item" data-id="${escapeHtml(c.id)}">
                <div class="conn-item-main">
                    <div class="conn-name">${escapeHtml(c.name)}</div>
                    <div class="conn-addr">${escapeHtml(c.host)}:${c.port}</div>
                </div>
                <button class="btn btn-sm btn-danger conn-del-btn" data-id="${escapeHtml(c.id)}" title="Delete">&times;</button>
            </li>
        `).join('');
    }

    async function connectTo(conn) {
      logDbg('connectTo ' + conn.host + ':' + conn.port);
      try {
        currentConnection = conn;
        API_BASE = `http://${conn.host}:${conn.port}`;
        try {
            await tauriInvoke('touch_connection', { id: conn.id });
        } catch (err) {
            // Non-fatal — we can still connect; just log.
            if (typeof console !== 'undefined') console.warn('touch_connection:', err);
        }

        // Load any previously-saved token for this specific connection.
        token = localStorage.getItem(tokenKey());
        currentUser = localStorage.getItem(userKey()) || '';

        // Probe reachability explicitly so unreachable hosts surface a clear error
        // instead of silently falling through to the login screen.
        let statusResp;
        try {
            logDbg('fetching ' + API_BASE + '/api/auth/status');
            statusResp = await fetch(API_BASE + '/api/auth/status', { method: 'GET' });
            logDbg('fetch status: ' + statusResp.status);
        } catch (e) {
            logDbg('fetch threw: ' + (e && e.message ? e.message : e));
            showConnError(`Could not reach ${conn.host}:${conn.port}: ${e && e.message ? e.message : e}`);
            return;
        }
        if (!statusResp.ok) {
            showConnError(`Server returned HTTP ${statusResp.status} — is this a Rust-DB server?`);
            return;
        }
        let statusData;
        try {
            statusData = await statusResp.json();
        } catch (e) {
            showConnError(`Server at ${conn.host}:${conn.port} is not a Rust-DB server (non-JSON response).`);
            return;
        }
        const authEnabled = !!statusData.auth_enabled;

        if (!authEnabled) {
            currentUser = 'anonymous';
            showApp();
            return;
        }
        if (await tryAutoLogin()) {
            showApp();
        } else {
            showScreen('login');
        }
      } catch (err) {
        showConnError('Connect failed: ' + (err && err.message ? err.message : String(err)));
        if (typeof console !== 'undefined') console.error('connectTo:', err);
      }
    }

    function showConnError(msg) {
        const el = $('#conn-error');
        el.textContent = msg;
        show(el);
    }

    function bindConnectionEvents() {
        // New connection buttons
        $('#conn-add-btn').addEventListener('click', () => showConnForm());
        $('#conn-add-empty-btn').addEventListener('click', () => showConnForm());
        $('#conn-cancel').addEventListener('click', () => {
            hide($('#conn-error'));
            showConnList();
        });

        // Save form
        $('#conn-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            logDbg('conn-form submit fired');
            try {
                hide($('#conn-error'));
                if (!tauriInvoke) {
                    showConnError('Tauri IPC not available. Reload the app.');
                    return;
                }
                const name = $('#conn-name').value.trim();
                const host = $('#conn-host').value.trim();
                const portStr = $('#conn-port').value.trim();
                const port = parseInt(portStr, 10);
                if (!name || !host || !Number.isFinite(port) || port < 1 || port > 65535) {
                    showConnError('Please provide a name, host, and valid port.');
                    return;
                }
                const conn = {
                    id: genId(),
                    name, host, port,
                    username: null,
                    last_used: null,
                };
                logDbg('calling save_connection ' + host + ':' + port);
                try {
                    await tauriInvoke('save_connection', { conn });
                    logDbg('save_connection ok');
                } catch (err) {
                    showConnError('Could not save connection: ' + (err && err.message ? err.message : String(err)));
                    return;
                }
                await connectTo(conn);
            } catch (err) {
                showConnError('Unexpected error: ' + (err && err.message ? err.message : String(err)));
                if (typeof console !== 'undefined') console.error('conn-form submit:', err);
            }
        });

        // Delegated click on list: select vs delete
        $('#conn-list').addEventListener('click', async (e) => {
            logDbg('conn-list click: ' + (e.target && e.target.tagName));
            try {
                hide($('#conn-error'));
                const delBtn = e.target.closest('.conn-del-btn');
                if (delBtn) {
                    e.stopPropagation();
                    e.preventDefault();
                    const id = delBtn.dataset.id;
                    // Note: window.confirm() is blocked in Tauri webviews, so
                    // delete immediately. Users can re-add via the form.
                    try {
                        await tauriInvoke('delete_connection', { id });
                    } catch (err) {
                        showConnError('Delete failed: ' + (err && err.message ? err.message : String(err)));
                        return;
                    }
                    await renderConnList();
                    return;
                }
                const item = e.target.closest('.conn-item');
                if (!item) return;
                const id = item.dataset.id;
                let conns;
                try {
                    conns = await tauriInvoke('list_connections');
                } catch (err) {
                    showConnError('Could not load connections: ' + (err && err.message ? err.message : String(err)));
                    return;
                }
                const conn = (conns || []).find(c => c.id === id);
                if (!conn) {
                    showConnError('Connection not found. Refresh and try again.');
                    return;
                }
                await connectTo(conn);
            } catch (err) {
                showConnError('Click failed: ' + (err && err.message ? err.message : String(err)));
                if (typeof console !== 'undefined') console.error('conn-list click:', err);
            }
        });
    }

    async function openConnectionPicker() {
        // Reset session state so switching connections doesn't leak auth.
        token = null;
        currentUser = '';
        currentConnection = null;
        API_BASE = '';
        hide($('#conn-error'));
        showConnList();
        await renderConnList();
        showScreen('connection');
    }

    // -----------------------------------------------------------------------
    // Settings modal
    // -----------------------------------------------------------------------

    function openSettings() {
        const modal = $('#settings-modal');
        if (!modal) return;
        // Sync form controls with current settings.
        const chk = $('#setting-show-debug');
        if (chk) chk.checked = !!settings.showDebug;
        const ps = $('#setting-page-size');
        if (ps) ps.value = String(getPageSize());
        // Populate about section.
        const connInfo = $('#settings-conn-info');
        if (connInfo) {
            connInfo.textContent = currentConnection
                ? `${currentConnection.name} (${currentConnection.host}:${currentConnection.port})`
                : (IS_TAURI ? 'Not connected' : 'Browser mode (same-origin)');
        }
        const rtInfo = $('#settings-runtime-info');
        if (rtInfo) rtInfo.textContent = IS_TAURI ? 'Tauri desktop' : 'Web browser';
        show(modal);
        // Move focus into the modal for keyboard users.
        const doneBtn = $('#settings-done');
        if (doneBtn) doneBtn.focus();
    }

    function closeSettings() {
        const modal = $('#settings-modal');
        if (modal) hide(modal);
    }

    function toggleDebugBar() {
        settings.showDebug = !settings.showDebug;
        saveSettings();
        applySettings();
    }

    function resetSettings() {
        for (const k of Object.keys(SETTINGS_DEFAULTS)) {
            settings[k] = SETTINGS_DEFAULTS[k];
        }
        saveSettings();
        applySettings();
        openSettings(); // re-sync the form controls
    }

    function bindSettingsEvents() {
        const modal = $('#settings-modal');
        if (!modal) return;

        $('#settings-btn').addEventListener('click', openSettings);
        $('#settings-close').addEventListener('click', closeSettings);
        $('#settings-done').addEventListener('click', closeSettings);
        $('#settings-reset').addEventListener('click', resetSettings);

        $('#setting-show-debug').addEventListener('change', (e) => {
            settings.showDebug = !!e.target.checked;
            saveSettings();
            applySettings();
        });

        $('#setting-page-size').addEventListener('change', (e) => {
            const n = parseInt(e.target.value, 10);
            if (Number.isFinite(n) && n >= 10 && n <= 1000) {
                settings.pageSize = n;
                saveSettings();
            } else {
                // Reject invalid input: snap back to the current value.
                e.target.value = String(getPageSize());
            }
        });

        // Click on overlay (not the inner modal) closes.
        modal.addEventListener('click', (e) => {
            if (e.target === modal) closeSettings();
        });
    }

    // -----------------------------------------------------------------------
    // Global hotkeys
    // -----------------------------------------------------------------------

    function isTextInputTarget(el) {
        if (!el) return false;
        const tag = el.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
        if (el.isContentEditable) return true;
        return false;
    }

    function bindHotkeys() {
        document.addEventListener('keydown', (e) => {
            const mod = e.ctrlKey || e.metaKey;

            // Esc: close modal (always — even if focus is in an input)
            if (e.key === 'Escape') {
                const modal = $('#settings-modal');
                if (modal && !modal.hidden) {
                    e.preventDefault();
                    closeSettings();
                    return;
                }
            }

            // Ctrl+, — open settings
            if (mod && e.key === ',') {
                e.preventDefault();
                openSettings();
                return;
            }

            // Ctrl+D — toggle debug bar (skip if user is typing in a field)
            if (mod && (e.key === 'd' || e.key === 'D') && !isTextInputTarget(e.target)) {
                e.preventDefault();
                toggleDebugBar();
                return;
            }

            // Ctrl+T — new query tab (only while on the SQL Console page)
            if (mod && (e.key === 't' || e.key === 'T')) {
                const queryPage = $('#page-query');
                if (queryPage && !queryPage.hidden) {
                    e.preventDefault();
                    newTab();
                    return;
                }
            }
        });
    }

    // Last-resort safety net: any unhandled async error in Tauri mode lands
    // in the connection error bar so failures are never invisible.
    if (IS_TAURI && typeof window !== 'undefined') {
        window.addEventListener('unhandledrejection', (ev) => {
            const reason = ev.reason;
            const msg = reason && reason.message ? reason.message : String(reason);
            const el = document.getElementById('conn-error');
            if (el) {
                el.textContent = 'Unhandled error: ' + msg;
                el.hidden = false;
            }
            if (typeof console !== 'undefined') console.error('unhandledrejection:', reason);
        });
    }

    // -----------------------------------------------------------------------
    // App initialization
    // -----------------------------------------------------------------------

    function showApp() {
        showScreen('app');
        $('#current-user').textContent = currentUser;
        if (IS_TAURI && currentConnection) {
            const connLabel = $('#current-conn');
            connLabel.textContent = ` @ ${currentConnection.name}`;
            show(connLabel);
            show($('#switch-conn-btn'));
        }
        loadTabs();
        // Populate the editor with the persisted active tab's content.
        const activeTab = queryTabs.find(t => t.id === activeTabId);
        if (activeTab) $('#sql-input').value = activeTab.sql;
        renderTabs();
        navigateTo('dashboard');
        renderHistory();
    }

    async function init() {
        logDbg('init() start');
        applySettings(); // honor showDebug before first paint
        bindEvents();
        bindSettingsEvents();
        bindHotkeys();
        logDbg('bindEvents done');

        if (IS_TAURI) {
            bindConnectionEvents();
            logDbg('bindConnectionEvents done, opening picker');
            await openConnectionPicker();
            logDbg('picker open');
            return;
        }

        // Browser mode: same-origin, no connection picker.
        const authEnabled = await checkAuthStatus();
        if (!authEnabled) {
            currentUser = 'anonymous';
            showApp();
            return;
        }
        if (await tryAutoLogin()) {
            showApp();
        } else {
            showScreen('login');
        }
    }

    // Start
    init();
})();
