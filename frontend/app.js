// Rust-DB Console — Frontend Application

(function () {
    'use strict';

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    let token = localStorage.getItem('rustdb_token');
    let currentUser = localStorage.getItem('rustdb_user') || '';
    let queryHistory = JSON.parse(localStorage.getItem('rustdb_history') || '[]');
    let isSignUp = false;

    // Table detail pagination
    let tableOffset = 0;
    const PAGE_SIZE = 100;
    let currentTable = '';
    let currentTableTotal = 0;

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
        const resp = await fetch(path, opts);
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
            localStorage.removeItem('rustdb_token');
            return false;
        }
    }

    async function login(username, password) {
        const resp = await api('POST', '/api/auth/login', { username, password });
        token = resp.token;
        currentUser = resp.username;
        localStorage.setItem('rustdb_token', token);
        localStorage.setItem('rustdb_user', currentUser);
    }

    async function logout() {
        try { await api('POST', '/api/auth/logout'); } catch { /* ignore */ }
        token = null;
        currentUser = '';
        localStorage.removeItem('rustdb_token');
        localStorage.removeItem('rustdb_user');
    }

    async function register(username, password) {
        const resp = await api('POST', '/api/auth/register', { username, password });
        token = resp.token;
        currentUser = resp.username;
        localStorage.setItem('rustdb_token', token);
        localStorage.setItem('rustdb_user', currentUser);
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
        if (name === 'login') {
            show($('#login-screen'));
            hide($('#main-app'));
        } else {
            hide($('#login-screen'));
            show($('#main-app'));
        }
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
            const data = await api('GET', `/api/tables/${encodeURIComponent(currentTable)}/data?limit=${PAGE_SIZE}&offset=${tableOffset}`);
            currentTableTotal = data.total;
            renderDataTable(wrap, data.columns, data.rows);
            updatePagination();
        } catch (e) {
            wrap.innerHTML = `<div class="error-msg">${escapeHtml(e.message)}</div>`;
        }
    }

    function updatePagination() {
        const end = Math.min(tableOffset + PAGE_SIZE, currentTableTotal);
        $('#data-info').textContent = currentTableTotal === 0
            ? 'No rows'
            : `Showing ${tableOffset + 1}-${end} of ${currentTableTotal}`;
        $('#data-prev').disabled = tableOffset === 0;
        $('#data-next').disabled = tableOffset + PAGE_SIZE >= currentTableTotal;
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

    async function executeQuery() {
        const sql = $('#sql-input').value.trim();
        if (!sql) return;

        hide($('#query-error'));
        hide($('#query-message'));
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
            showScreen('login');
        });

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
            tableOffset = Math.max(0, tableOffset - PAGE_SIZE);
            loadTableData();
        });
        $('#data-next').addEventListener('click', () => {
            tableOffset += PAGE_SIZE;
            loadTableData();
        });

        // SQL Console
        $('#run-query').addEventListener('click', executeQuery);
        $('#clear-query').addEventListener('click', () => {
            $('#sql-input').value = '';
            hide($('#query-error'));
            hide($('#query-message'));
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

        // Query history click
        $('#query-history').addEventListener('click', (e) => {
            const li = e.target.closest('li');
            if (li) {
                const idx = parseInt(li.dataset.index);
                if (queryHistory[idx]) {
                    $('#sql-input').value = queryHistory[idx];
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
    // App initialization
    // -----------------------------------------------------------------------

    function showApp() {
        showScreen('app');
        $('#current-user').textContent = currentUser;
        navigateTo('dashboard');
        renderHistory();
    }

    async function init() {
        bindEvents();

        const authEnabled = await checkAuthStatus();

        if (!authEnabled) {
            // Auth disabled — go straight to the app
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
