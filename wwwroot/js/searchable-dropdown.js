/**
 * Searchable Multi-Select Dropdown Component
 * Features: Search, Select All, Checkboxes, Responsive
 * Usage: Add class="multi-select" to any <select> element
 *        Add class="search-select" for single-select with search
 */
(function () {
    'use strict';

    function initSearchableDropdowns() {
        document.querySelectorAll('select.multi-select').forEach(el => {
            if (el.dataset.enhanced) return;
            el.dataset.enhanced = 'true';
            new MultiSelectDropdown(el);
        });
        document.querySelectorAll('select.search-select').forEach(el => {
            if (el.dataset.enhanced) return;
            el.dataset.enhanced = 'true';
            new SearchSelectDropdown(el);
        });
    }

    // ===== MULTI-SELECT WITH CHECKBOXES =====
    function MultiSelectDropdown(selectEl) {
        var self = this;
        self.select = selectEl;
        self.name = selectEl.name;
        self.options = [];
        self.selected = new Set();

        // Parse options from original select
        Array.from(selectEl.options).forEach(function (opt) {
            if (opt.value === '') return; // skip placeholder
            self.options.push({ value: opt.value, text: opt.textContent, selected: opt.selected });
            if (opt.selected && opt.value) self.selected.add(opt.value);
        });

        // Hide original select and remove attributes that cause validation errors
        selectEl.style.display = 'none';
        selectEl.removeAttribute('name');
        selectEl.removeAttribute('required');
        selectEl.removeAttribute('data-val-required');

        // Create hidden input for form submission
        self.hiddenInput = document.createElement('input');
        self.hiddenInput.type = 'hidden';
        self.hiddenInput.name = self.name;
        self.hiddenInput.value = Array.from(self.selected).join(',');
        selectEl.parentNode.insertBefore(self.hiddenInput, selectEl.nextSibling);

        // Build UI
        self.wrapper = document.createElement('div');
        self.wrapper.className = 'ms-dropdown-wrapper';
        selectEl.parentNode.insertBefore(self.wrapper, selectEl.nextSibling);

        // Trigger button
        self.trigger = document.createElement('div');
        self.trigger.className = 'ms-dropdown-trigger form-select form-select-sm';
        self.trigger.tabIndex = 0;
        self.wrapper.appendChild(self.trigger);
        self.updateTriggerText();

        // Dropdown panel
        self.panel = document.createElement('div');
        self.panel.className = 'ms-dropdown-panel';
        self.wrapper.appendChild(self.panel);

        // Search input
        var searchWrap = document.createElement('div');
        searchWrap.className = 'ms-dropdown-search';
        searchWrap.innerHTML = '<input type="text" class="form-control form-control-sm" placeholder="Search...">';
        self.panel.appendChild(searchWrap);
        self.searchInput = searchWrap.querySelector('input');

        // Select All
        var allWrap = document.createElement('div');
        allWrap.className = 'ms-dropdown-item ms-dropdown-selectall';
        allWrap.innerHTML = '<label><input type="checkbox" class="form-check-input me-2"> <strong>Select All</strong></label>';
        self.panel.appendChild(allWrap);
        self.selectAllCb = allWrap.querySelector('input');

        // Options container
        self.optionsContainer = document.createElement('div');
        self.optionsContainer.className = 'ms-dropdown-options';
        self.panel.appendChild(self.optionsContainer);

        // Render options
        self.renderOptions('');

        // Events
        self.trigger.addEventListener('click', function (e) {
            e.stopPropagation();
            self.toggle();
        });

        self.searchInput.addEventListener('input', function () {
            self.renderOptions(this.value.toLowerCase());
        });

        self.searchInput.addEventListener('click', function (e) { e.stopPropagation(); });

        self.selectAllCb.addEventListener('change', function () {
            var checked = this.checked;
            var visibleCheckboxes = self.optionsContainer.querySelectorAll('input[type="checkbox"]:not([style*="display: none"])');
            // Get all visible items
            self.optionsContainer.querySelectorAll('.ms-dropdown-item').forEach(function (item) {
                if (item.style.display !== 'none') {
                    var cb = item.querySelector('input[type="checkbox"]');
                    if (cb) {
                        cb.checked = checked;
                        if (checked) self.selected.add(cb.value);
                        else self.selected.delete(cb.value);
                    }
                }
            });
            self.updateHiddenInput();
            self.updateTriggerText();
        });

        self.panel.addEventListener('click', function (e) { e.stopPropagation(); });

        document.addEventListener('click', function () { self.close(); });

        // Clear button
        var btnWrap = document.createElement('div');
        btnWrap.className = 'ms-dropdown-actions';
        btnWrap.innerHTML = '<button type="button" class="btn btn-sm btn-outline-secondary w-100 py-0">Clear All</button>';
        self.panel.appendChild(btnWrap);
        btnWrap.querySelector('button').addEventListener('click', function () {
            self.selected.clear();
            self.optionsContainer.querySelectorAll('input[type="checkbox"]').forEach(function (cb) { cb.checked = false; });
            self.selectAllCb.checked = false;
            self.updateHiddenInput();
            self.updateTriggerText();
        });
    }

    MultiSelectDropdown.prototype.renderOptions = function (filter) {
        var self = this;
        self.optionsContainer.innerHTML = '';
        var count = 0;
        self.options.forEach(function (opt) {
            var show = !filter || opt.text.toLowerCase().indexOf(filter) !== -1 || opt.value.toLowerCase().indexOf(filter) !== -1;
            var div = document.createElement('div');
            div.className = 'ms-dropdown-item';
            if (!show) div.style.display = 'none';
            else count++;
            var checked = self.selected.has(opt.value) ? 'checked' : '';
            div.innerHTML = '<label><input type="checkbox" class="form-check-input me-2" value="' + opt.value + '" ' + checked + '> ' + opt.text + '</label>';
            var cb = div.querySelector('input');
            cb.addEventListener('change', function () {
                if (this.checked) self.selected.add(this.value);
                else self.selected.delete(this.value);
                self.updateHiddenInput();
                self.updateTriggerText();
                self.updateSelectAllState();
            });
            self.optionsContainer.appendChild(div);
        });
        if (count === 0) {
            var empty = document.createElement('div');
            empty.className = 'ms-dropdown-empty';
            empty.textContent = 'No matches found';
            self.optionsContainer.appendChild(empty);
        }
        self.updateSelectAllState();
    };

    MultiSelectDropdown.prototype.updateSelectAllState = function () {
        var visible = this.optionsContainer.querySelectorAll('.ms-dropdown-item:not([style*="display: none"]) input[type="checkbox"]');
        var allChecked = visible.length > 0;
        visible.forEach(function (cb) { if (!cb.checked) allChecked = false; });
        this.selectAllCb.checked = allChecked;
    };

    MultiSelectDropdown.prototype.updateHiddenInput = function () {
        this.hiddenInput.value = Array.from(this.selected).join(',');
    };

    MultiSelectDropdown.prototype.updateTriggerText = function () {
        var n = this.selected.size;
        if (n === 0) this.trigger.innerHTML = '<span class="text-muted">-- All --</span>';
        else if (n === 1) this.trigger.textContent = Array.from(this.selected)[0];
        else if (n <= 3) this.trigger.textContent = Array.from(this.selected).join(', ');
        else this.trigger.textContent = n + ' selected';
    };

    MultiSelectDropdown.prototype.toggle = function () {
        var open = this.panel.classList.toggle('show');
        if (open) { this.searchInput.value = ''; this.renderOptions(''); this.searchInput.focus(); }
    };

    MultiSelectDropdown.prototype.close = function () {
        this.panel.classList.remove('show');
    };

    // ===== SINGLE-SELECT WITH SEARCH =====
    function SearchSelectDropdown(selectEl) {
        var self = this;
        self.select = selectEl;
        self.name = selectEl.name;
        self.options = [];
        self.currentValue = selectEl.value;
        self.currentText = selectEl.options[selectEl.selectedIndex]?.textContent || '';

        Array.from(selectEl.options).forEach(function (opt) {
            self.options.push({ value: opt.value, text: opt.textContent });
        });

        selectEl.style.display = 'none';
        selectEl.removeAttribute('name');
        selectEl.removeAttribute('required');
        selectEl.removeAttribute('data-val-required');

        self.hiddenInput = document.createElement('input');
        self.hiddenInput.type = 'hidden';
        self.hiddenInput.name = self.name;
        self.hiddenInput.value = self.currentValue;
        selectEl.parentNode.insertBefore(self.hiddenInput, selectEl.nextSibling);

        self.wrapper = document.createElement('div');
        self.wrapper.className = 'ms-dropdown-wrapper';
        selectEl.parentNode.insertBefore(self.wrapper, selectEl.nextSibling);

        self.trigger = document.createElement('div');
        self.trigger.className = 'ms-dropdown-trigger form-select form-select-sm';
        self.trigger.tabIndex = 0;
        self.trigger.textContent = self.currentText || self.options[0]?.text || 'Select...';
        self.wrapper.appendChild(self.trigger);

        self.panel = document.createElement('div');
        self.panel.className = 'ms-dropdown-panel';
        self.wrapper.appendChild(self.panel);

        var searchWrap = document.createElement('div');
        searchWrap.className = 'ms-dropdown-search';
        searchWrap.innerHTML = '<input type="text" class="form-control form-control-sm" placeholder="Search...">';
        self.panel.appendChild(searchWrap);
        self.searchInput = searchWrap.querySelector('input');

        self.optionsContainer = document.createElement('div');
        self.optionsContainer.className = 'ms-dropdown-options';
        self.panel.appendChild(self.optionsContainer);

        self.renderOptions('');

        self.trigger.addEventListener('click', function (e) { e.stopPropagation(); self.toggle(); });
        self.searchInput.addEventListener('input', function () { self.renderOptions(this.value.toLowerCase()); });
        self.searchInput.addEventListener('click', function (e) { e.stopPropagation(); });
        self.panel.addEventListener('click', function (e) { e.stopPropagation(); });
        document.addEventListener('click', function () { self.close(); });
    }

    SearchSelectDropdown.prototype.renderOptions = function (filter) {
        var self = this;
        self.optionsContainer.innerHTML = '';
        var count = 0;
        self.options.forEach(function (opt) {
            var show = !filter || opt.text.toLowerCase().indexOf(filter) !== -1 || opt.value.toLowerCase().indexOf(filter) !== -1;
            if (!show) return;
            count++;
            var div = document.createElement('div');
            div.className = 'ms-dropdown-item ms-dropdown-single-item';
            if (opt.value === self.currentValue) div.classList.add('active');
            div.textContent = opt.text;
            div.addEventListener('click', function () {
                self.currentValue = opt.value;
                self.currentText = opt.text;
                self.hiddenInput.value = opt.value;
                self.trigger.textContent = opt.text;
                self.close();
                // Fire onchange on original select + hidden input for callbacks like updateTableInfo
                self.select.value = opt.value;
                self.select.dispatchEvent(new Event('change'));
                self.hiddenInput.dispatchEvent(new Event('change'));
                // Also call any onchange attribute directly
                if (self.select.getAttribute('onchange')) {
                    try { new Function('value', self.select.getAttribute('onchange').replace('this.value','value'))(opt.value); } catch(e){}
                }
            });
            self.optionsContainer.appendChild(div);
        });
        if (count === 0) {
            var empty = document.createElement('div');
            empty.className = 'ms-dropdown-empty';
            empty.textContent = 'No matches found';
            self.optionsContainer.appendChild(empty);
        }
    };

    SearchSelectDropdown.prototype.toggle = function () {
        var open = this.panel.classList.toggle('show');
        if (open) { this.searchInput.value = ''; this.renderOptions(''); this.searchInput.focus(); }
    };

    SearchSelectDropdown.prototype.close = function () {
        this.panel.classList.remove('show');
    };

    // Init on DOM ready
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initSearchableDropdowns);
    else initSearchableDropdowns();

    // Re-init after dynamic content
    window.initSearchableDropdowns = initSearchableDropdowns;
})();
