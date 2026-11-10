class HistoryRebuildManager {
    static STATUS_META = {
        rebuilt: { label: 'Rebuilt', badge: 'badge-success', icon: 'bi-check-circle' },
        would_rebuild: { label: 'Would Rebuild', badge: 'badge-info', icon: 'bi-clock' },
        exists: { label: 'Already Have', badge: 'badge-warning', icon: 'bi-hdd' },
        already_have: { label: 'Already Have', badge: 'badge-warning', icon: 'bi-hdd' },
        no_history: { label: 'No History', badge: 'badge-ghost', icon: 'bi-slash-circle' },
        not_torrent: { label: 'Not Torrent', badge: 'badge-ghost', icon: 'bi-file-earmark-x' },
        failed: { label: 'Failed', badge: 'badge-error', icon: 'bi-x-circle' },
    };

    constructor() {
        this.api = `${window.urlBase}api`;
        this.running = false;
        this.report = null;
        this.results = [];
        this.filteredResults = [];
        this.page = 1;
        this.pageSize = 15;
        this.searchDebounce = null;

        this.bind();
        this.loadArrs();
    }

    bind() {
        const $ = (id) => document.getElementById(id);

        $('runRebuildBtn')?.addEventListener('click', () => this.runRebuild());
        $('resultsStatusFilter')?.addEventListener('change', () => this.onFilterChange());
        $('resultsSearchInput')?.addEventListener('input', () => {
            clearTimeout(this.searchDebounce);
            this.searchDebounce = setTimeout(() => this.onFilterChange(), 200);
        });
        $('historyDryRun')?.addEventListener('change', (e) => {
            const label = $('statRebuiltLabel');
            if (label && !this.report) {
                label.innerHTML = e.target.checked
                    ? '<i class="bi bi-clock mr-1"></i>Would Rebuild'
                    : '<i class="bi bi-check-circle mr-1"></i>Rebuilt';
            }
        });
    }

    async loadArrs() {
        const select = document.getElementById('historyArrSelect');
        if (!select) return;

        try {
            const arrs = await this.fetchJSON(`${this.api}/arrs`) || [];
            // Filter to Radarr and Sonarr
            const eligible = arrs.filter((a) => {
                const t = String(a.type || '').toLowerCase();
                return t === 'radarr' || t === 'sonarr';
            });

            select.innerHTML = '<option value="">All eligible Arrs (Radarr &amp; Sonarr)</option>' +
                eligible.map((a) => `<option value="${this.escapeAttr(a.name)}">${this.escape(a.name)} (${this.escape(a.type)})</option>`).join('');
        } catch (e) {
            console.warn('Failed to load Arrs list:', e);
        }
    }

    async runRebuild() {
        if (this.running) return;

        const btn = document.getElementById('runRebuildBtn');
        const indicator = document.getElementById('runProgressIndicator');
        const errorAlert = document.getElementById('rebuildErrorAlert');
        const arrSelect = document.getElementById('historyArrSelect');
        const dryRunCheck = document.getElementById('historyDryRun');
        const statusLine = document.getElementById('historyStatusLine');

        const arr = arrSelect ? arrSelect.value : '';
        const dryRun = dryRunCheck ? dryRunCheck.checked : true;

        this.running = true;
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="loading loading-spinner loading-xs mr-1"></span>Running…';
        }
        if (indicator) indicator.classList.remove('hidden');
        if (errorAlert) errorAlert.classList.add('hidden');
        if (statusLine) {
            statusLine.textContent = dryRun
                ? 'Simulating rebuild pass from Arr history…'
                : 'Running rebuild pass from Arr history…';
        }

        try {
            const params = new URLSearchParams();
            if (arr) params.set('arr', arr);
            if (dryRun) params.set('dry_run', 'true');

            const res = await fetch(`${this.api}/repair/rebuild-from-history?${params.toString()}`, {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Accept': 'application/json' }
            });

            if (!res.ok) {
                const errText = await res.text();
                let message = `HTTP ${res.status}`;
                try {
                    const parsed = JSON.parse(errText);
                    message = parsed.error || parsed.message || message;
                } catch {
                    if (errText && errText.trim().length > 0) message = errText;
                }
                throw new Error(message);
            }

            const data = await res.json();
            this.report = data;
            this.results = data.results || [];

            this.renderStats(data);
            this.onFilterChange();

            const count = data.dry_run ? (data.would_rebuild ?? 0) : (data.rebuilt ?? 0);
            const label = data.dry_run ? 'would be rebuilt' : 'rebuilt';
            this.toast(`Rebuild finished: ${count} ${label}, ${data.skipped ?? 0} skipped, ${data.failed ?? 0} failed`, count > 0 ? 'success' : 'info');

            if (statusLine) {
                statusLine.textContent = `Completed pass (${data.dry_run ? 'Dry Run' : 'Live'}): examined ${data.arr_files_found} files, ${count} ${label}.`;
            }
        } catch (err) {
            console.error('History rebuild error:', err);
            if (errorAlert) {
                errorAlert.textContent = `Error: ${err.message || err}`;
                errorAlert.classList.remove('hidden');
            }
            if (statusLine) {
                statusLine.textContent = 'Pass failed. See error details above.';
            }
            this.toast(err.message || 'Failed to run rebuild pass', 'error');
        } finally {
            this.running = false;
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-play-fill mr-1"></i>Run Rebuild';
            }
            if (indicator) indicator.classList.add('hidden');
        }
    }

    renderStats(report) {
        const $ = (id) => document.getElementById(id);

        if ($('statArrFiles')) $('statArrFiles').textContent = report.arr_files_found ?? 0;
        if ($('statLocal')) $('statLocal').textContent = report.local ?? 0;
        if ($('statUnresolved')) $('statUnresolved').textContent = report.unresolved ?? 0;
        if ($('statScanned')) $('statScanned').textContent = report.scanned ?? 0;

        const isDryRun = Boolean(report.dry_run);
        const rebuiltCount = isDryRun ? (report.would_rebuild ?? 0) : (report.rebuilt ?? 0);

        if ($('statRebuilt')) $('statRebuilt').textContent = rebuiltCount;
        if ($('statRebuiltLabel')) {
            $('statRebuiltLabel').innerHTML = isDryRun
                ? '<i class="bi bi-clock mr-1"></i>Would Rebuild'
                : '<i class="bi bi-check-circle mr-1"></i>Rebuilt';
        }

        if ($('statSkipped')) $('statSkipped').textContent = report.skipped ?? 0;
        if ($('statFailed')) $('statFailed').textContent = report.failed ?? 0;

        // Skip breakdown panel
        const skipPanel = $('skipBreakdownPanel');
        if (skipPanel) {
            if ((report.skipped ?? 0) > 0) {
                skipPanel.classList.remove('hidden');
                if ($('skipNoHistory')) $('skipNoHistory').textContent = `No grab history: ${report.no_history ?? 0}`;
                if ($('skipNotTorrent')) $('skipNotTorrent').textContent = `Non-torrent grab: ${report.not_torrent ?? 0}`;
                if ($('skipAlreadyHave')) $('skipAlreadyHave').textContent = `Infohash already present: ${report.already_have ?? 0}`;
            } else {
                skipPanel.classList.add('hidden');
            }
        }
    }

    onFilterChange() {
        const filterVal = document.getElementById('resultsStatusFilter')?.value || '';
        const searchVal = (document.getElementById('resultsSearchInput')?.value || '').toLowerCase().trim();

        this.filteredResults = this.results.filter((item) => {
            // Status filter
            if (filterVal) {
                const s = (item.status || '').toLowerCase();
                if (filterVal === 'rebuilt') {
                    if (s !== 'rebuilt' && s !== 'would_rebuild') return false;
                } else if (filterVal === 'skipped') {
                    if (s !== 'no_history' && s !== 'not_torrent' && s !== 'already_have' && s !== 'exists') return false;
                } else if (filterVal === 'failed') {
                    if (s !== 'failed') return false;
                }
            }

            // Search filter
            if (searchVal) {
                const folderMatch = (item.folder || '').toLowerCase().includes(searchVal);
                const hashMatch = (item.infohash || '').toLowerCase().includes(searchVal);
                const arrMatch = (item.arr || '').toLowerCase().includes(searchVal);
                if (!folderMatch && !hashMatch && !arrMatch) return false;
            }

            return true;
        });

        this.page = 1;
        this.renderTable();
    }

    renderTable() {
        const body = document.getElementById('resultsTableBody');
        const badge = document.getElementById('resultsCountBadge');
        if (!body) return;

        const total = this.filteredResults.length;
        if (badge) badge.textContent = `${total} item${total === 1 ? '' : 's'}`;

        if (total === 0) {
            if (!this.report) {
                body.innerHTML = '<tr><td colspan="5" class="text-center py-8 opacity-60">No rebuild has been run yet. Configure the options above and click "Run Rebuild".</td></tr>';
            } else {
                body.innerHTML = '<tr><td colspan="5" class="text-center py-8 opacity-60">No results match your filter.</td></tr>';
            }
            this.renderPagination({ total: 0, totalPages: 1, offset: 0, itemsCount: 0 });
            return;
        }

        const totalPages = Math.max(1, Math.ceil(total / this.pageSize));
        this.page = Math.min(Math.max(1, this.page), totalPages);
        const offset = (this.page - 1) * this.pageSize;
        const pageItems = this.filteredResults.slice(offset, offset + this.pageSize);

        body.innerHTML = pageItems.map((item) => {
            const meta = HistoryRebuildManager.STATUS_META[item.status] || {
                label: item.status || 'unknown',
                badge: 'badge-ghost',
                icon: 'bi-question'
            };

            const infohash = item.infohash || '';
            const error = item.error || '';

            return `
                <tr>
                    <td>
                        <span class="badge ${meta.badge} badge-sm gap-1">
                            <i class="bi ${meta.icon}"></i>${this.escape(meta.label)}
                        </span>
                    </td>
                    <td><span class="text-xs font-semibold">${this.escape(item.arr || '-')}</span></td>
                    <td class="max-w-md">
                        <div class="truncate font-medium text-xs sm:text-sm" title="${this.escapeAttr(item.folder)}">
                            ${this.escape(item.folder)}
                        </div>
                    </td>
                    <td>
                        ${infohash ? `
                            <div class="flex items-center gap-1">
                                <code class="text-xs font-mono select-all truncate max-w-[140px] sm:max-w-[180px]" title="${this.escapeAttr(infohash)}">
                                    ${this.escape(infohash)}
                                </code>
                                <button type="button" class="btn btn-ghost btn-xs p-1 h-auto min-h-0"
                                        title="Copy infohash" onclick="window.historyRebuildManager.copyText('${this.escapeAttr(infohash)}')">
                                    <i class="bi bi-clipboard text-xs"></i>
                                </button>
                            </div>
                        ` : '<span class="opacity-40 text-xs">-</span>'}
                    </td>
                    <td class="max-w-xs">
                        ${error ? `
                            <div class="text-xs text-error truncate" title="${this.escapeAttr(error)}">
                                ${this.escape(error)}
                            </div>
                        ` : (item.status === 'rebuilt' || item.status === 'would_rebuild' ? `
                            <span class="text-xs text-success/80">Recovered from grab history</span>
                        ` : (item.status === 'no_history' ? `
                            <span class="text-xs opacity-60">No torrent grab in Arr history</span>
                        ` : (item.status === 'not_torrent' ? `
                            <span class="text-xs opacity-60">Non-torrent downloadId</span>
                        ` : (item.status === 'already_have' || item.status === 'exists' ? `
                            <span class="text-xs opacity-60">Entry already in storage</span>
                        ` : '<span class="opacity-40 text-xs">-</span>'))))}
                    </td>
                </tr>
            `;
        }).join('');

        this.renderPagination({
            total,
            totalPages,
            offset,
            itemsCount: pageItems.length
        });
    }

    renderPagination(page) {
        const info = document.getElementById('resultsPaginationInfo');
        const controls = document.getElementById('resultsPaginationControls');
        if (!info || !controls) return;

        if (page.total === 0) {
            info.textContent = 'Showing 0 of 0';
            controls.innerHTML = '';
            return;
        }

        const start = page.offset + 1;
        const end = Math.min(page.offset + page.itemsCount, page.total);
        info.textContent = `Showing ${start}-${end} of ${page.total} items`;

        if (page.totalPages <= 1) {
            controls.innerHTML = '';
            return;
        }

        let html = `
            <button class="join-item btn btn-sm" onclick="window.historyRebuildManager.goToPage(${this.page - 1})"
                    aria-label="Previous page" ${this.page === 1 ? 'disabled' : ''}>
                <i class="bi bi-chevron-left"></i>
            </button>`;

        const pageNumbers = new Set([1, page.totalPages]);
        for (let num = this.page - 2; num <= this.page + 2; num++) {
            if (num > 0 && num <= page.totalPages) pageNumbers.add(num);
        }

        let prev = 0;
        [...pageNumbers].sort((a, b) => a - b).forEach((num) => {
            if (prev && num > prev + 1) {
                html += '<button class="join-item btn btn-sm" disabled aria-hidden="true">…</button>';
            }
            html += `
                <button class="join-item btn btn-sm ${num === this.page ? 'btn-active' : ''}"
                        onclick="window.historyRebuildManager.goToPage(${num})">
                    ${num}
                </button>`;
            prev = num;
        });

        html += `
            <button class="join-item btn btn-sm" onclick="window.historyRebuildManager.goToPage(${this.page + 1})"
                    aria-label="Next page" ${this.page === page.totalPages ? 'disabled' : ''}>
                <i class="bi bi-chevron-right"></i>
            </button>`;

        controls.innerHTML = html;
    }

    goToPage(page) {
        this.page = page;
        this.renderTable();
    }

    async copyText(text) {
        try {
            await navigator.clipboard.writeText(text);
            this.toast('InfoHash copied to clipboard', 'info');
        } catch {
            this.toast('Failed to copy', 'error');
        }
    }

    async fetchJSON(url) {
        const res = await fetch(url, { credentials: 'same-origin' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
    }

    escape(str) {
        if (str == null) return '';
        const d = document.createElement('div');
        d.textContent = String(str);
        return d.innerHTML;
    }

    escapeAttr(str) {
        return this.escape(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    toast(msg, type = 'info') {
        if (typeof window.decypharrUtils?.createToast === 'function') {
            window.decypharrUtils.createToast(msg, type);
        } else {
            console.log(`[${type}] ${msg}`);
        }
    }
}

window.HistoryRebuildManager = HistoryRebuildManager;
