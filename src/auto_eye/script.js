const INDEXES = {
  GER40: { quote: 'EUR', contractSize: 25 },
  SPX500: { quote: 'USD', contractSize: 50 },
  NAS100: { quote: 'USD', contractSize: 50 }
};

function formatDateTime(value) {
  if (value === null || value === undefined || value === '') return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  const pad = (num) => String(num).padStart(2, '0');
  const day = pad(date.getDate());
  const month = pad(date.getMonth() + 1);
  const year = date.getFullYear();
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  return `${day}.${month}.${year}, ${hours}:${minutes}`;
}

// ==== вспомогательные словари ====
const CONTRACT_SIZES = {
  'XAUUSD': 100,
  'XAGUSD': 5000,
  'BTCUSDT': 1,
  'ETHUSDT': 1,
  DEFAULT: 100000
};

// ==== разбор пары/тика ====
function parseCurrencyPair(pair) {
  const p = pair.toUpperCase();
  if (INDEXES[p]) {
    return { base: p, quote: INDEXES[p].quote };
  }
  if (p.includes('/')) {
    const [base, quote] = p.split('/');
    return { base, quote };
  }
  if (p.endsWith('USDT')) return { base: p.slice(0, -4), quote: 'USDT' };
  if (p.endsWith('USD')) return { base: p.slice(0, -3), quote: 'USD' };
  throw new Error('Неверный формат актива.');
}

// ==== контракт-сайз для расчётов ====
function getContractSize(pair) {
  const key = pair.replace('/', '').toUpperCase();
  if (INDEXES[key]) return INDEXES[key].contractSize;
  return CONTRACT_SIZES[key] || CONTRACT_SIZES.DEFAULT;
}

// ==== определяем тип актива ====
function isCryptoPair(pair) {
  return /BTC|ETH/i.test(pair);
}
function isIndexPair(pair) {
  return Boolean(INDEXES[pair.toUpperCase()]);
}

// ==== API курсов (стейблкоины нормализуются к USD) ====
async function getExchangeRate(from, to) {
  const STABLE = ['USDT', 'USDC', 'DAI', 'TUSD', 'USDP'];
  const norm = c => STABLE.includes(c) ? 'USD' : c;
  const f = norm(from.toUpperCase()), t = norm(to.toUpperCase());
  if (f === t) return 1;
  try {
    const res = await fetch(`https://open.er-api.com/v6/latest/${f}`);
    const data = await res.json();
    if (data.result !== 'success' || !data.rates[t])
      throw new Error(`${from}/${to} не найден`);
    return data.rates[t];
  } catch (err) {
    alert('Не удалось получить курсы: ' + err.message);
    throw err;
  }
}

// ==== сохранение/загрузка формы ====
function saveFormValues() {
  [
    'currencyPair', 'depositCurrency', 'deposit', 'openPrice', 'takeProfit',
    'stopLoss', 'lots', 'leverage', 'dealComment'
  ].forEach(id => {
    const el = document.getElementById(id);
    if (el) localStorage.setItem(`calc_${id}`, el.value);
  });
}

function loadFormValues() {
  [
    'currencyPair', 'depositCurrency', 'deposit', 'openPrice', 'takeProfit',
    'stopLoss', 'lots', 'leverage', 'dealComment'
  ].forEach(id => {
    const stored = localStorage.getItem(`calc_${id}`);
    if (stored !== null) {
      const el = document.getElementById(id);
      if (el) el.value = stored;
    }
  });
}

// ==== сохранённые сделки ====
let savedDeals = [];
let lastCalculation = null;
let editingDealId = null;
const DEALS_KEY = 'calc_savedDeals';

function saveDealsToStorage() {
  try {
    localStorage.setItem(DEALS_KEY, JSON.stringify(savedDeals));
  } catch (err) {
    console.error('Не удалось сохранить сделки', err);
  }
}

function renderSavedDeals() {
  const tbody = document.getElementById('savedDealsBody');
  const noMsg = document.getElementById('noDealsMessage');
  const table = document.getElementById('savedDealsTable');
  if (!tbody || !noMsg || !table) return;

  let deals = [...savedDeals];

  const sortBySel = document.getElementById('sortBy');
  const sortOrderSel = document.getElementById('sortOrder');
  const sortBy = sortBySel ? sortBySel.value : 'created';
  const sortOrder = sortOrderSel ? sortOrderSel.value : 'desc';
  const factor = sortOrder === 'asc' ? 1 : -1;

  deals.sort((a, b) => {
    let av, bv;
    if (sortBy === 'created') {
      av = a.created || 0;
      bv = b.created || 0;
    } else {
      av = typeof a[sortBy] === 'number' ? a[sortBy] : 0;
      bv = typeof b[sortBy] === 'number' ? b[sortBy] : 0;
    }
    if (av === bv) return 0;
    return av > bv ? factor : -factor;
  });

  tbody.innerHTML = '';

  deals.forEach((d, index) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${index + 1}</td>
      <td>${d.pair}</td>
      <td>${d.direction}</td>
      <td>${d.lots}</td>
      <td>${d.leverage}x</td>
      <td>${formatDateTime(d.created)}</td>
      <td class="profit">${d.profit.toFixed(2)} ${d.depCur}</td>
      <td class="loss">${d.loss.toFixed(2)} ${d.depCur}</td>
      <td class="profit">${d.profitDepPct.toFixed(2)} %</td>
      <td class="loss">${d.lossDepPct.toFixed(2)} %</td>
      <td class="profit">${d.profitPosPct.toFixed(2)} %</td>
      <td class="loss">${d.lossPosPct.toFixed(2)} %</td>
      <td>${d.margin.toFixed(2)} ${d.depCur}</td>
      <td class="deal-comment">
        <textarea data-id="${d.id}" placeholder="Комментарий к сделке"></textarea>
      </td>
      <td class="deal-actions">
        <button type="button" data-action="edit" data-id="${d.id}" class="secondary-btn">Редактировать</button>
        <button type="button" data-action="delete" data-id="${d.id}" class="secondary-btn">Удалить</button>
      </td>
    `;
    tbody.appendChild(tr);

    // безопасно проставляем текст комментария уже после innerHTML
    const ta = tr.querySelector('textarea[data-id]');
    if (ta) ta.value = d.comment || '';
  });

  noMsg.style.display = deals.length ? 'none' : 'block';
  table.style.display = deals.length ? 'table' : 'none';
}

function loadSavedDealsFromStorage() {
  try {
    const raw = localStorage.getItem(DEALS_KEY);
    if (!raw) {
      savedDeals = [];
    } else {
      const parsed = JSON.parse(raw);
      savedDeals = Array.isArray(parsed) ? parsed : [];
    }
  } catch (err) {
    console.error('Не удалось прочитать сохранённые сделки', err);
    savedDeals = [];
  }
  renderSavedDeals();
}

function deleteDeal(id) {
  savedDeals = savedDeals.filter(d => String(d.id) !== String(id));
  saveDealsToStorage();
  renderSavedDeals();
}

function startEditDeal(id) {
  const deal = savedDeals.find(d => String(d.id) === String(id));
  if (!deal) return;

  editingDealId = String(id);
  lastCalculation = { ...deal };

  const pairEl = document.getElementById('currencyPair');
  if (pairEl && deal.pair) pairEl.value = deal.pair;
  const depCurEl = document.getElementById('depositCurrency');
  if (depCurEl && deal.depCur) depCurEl.value = deal.depCur;
  const depEl = document.getElementById('deposit');
  if (depEl && deal.dep !== undefined) depEl.value = deal.dep;
  const openEl = document.getElementById('openPrice');
  if (openEl && deal.open !== undefined) openEl.value = deal.open;
  const tpEl = document.getElementById('takeProfit');
  if (tpEl && deal.tp !== undefined) tpEl.value = deal.tp;
  const slEl = document.getElementById('stopLoss');
  if (slEl && deal.sl !== undefined) slEl.value = deal.sl;
  const lotsEl = document.getElementById('lots');
  if (lotsEl && deal.lots !== undefined) lotsEl.value = deal.lots;
  const levEl = document.getElementById('leverage');
  if (levEl && deal.leverage !== undefined) levEl.value = deal.leverage;
  const commentInput = document.getElementById('dealComment');
  if (commentInput) commentInput.value = deal.comment || '';

  updateAssetTheme();
  updateMaxLots();

  Promise.resolve(calculate({ preventDefault: () => {} }))
    .finally(() => {
      const saveBtn = document.getElementById('saveDealBtn');
      if (saveBtn) {
        saveBtn.disabled = false;
        saveBtn.textContent = 'Обновить сделку';
      }
    });
}

function ensureDealIds(deals) {
  const usedIds = new Set(savedDeals.map(d => String(d.id)));
  deals.forEach((deal) => {
    if (!deal || typeof deal !== 'object') return;
    let id = deal.id ? String(deal.id) : '';
    if (!id || usedIds.has(id)) {
      id = `${Date.now()}_${Math.floor(Math.random() * 100000)}`;
      deal.id = id;
    }
    usedIds.add(id);
  });
}

function importDealsFromText(text) {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    alert('Не удалось разобрать текст импорта.');
    return;
  }

  let deals = null;
  if (Array.isArray(parsed)) {
    deals = parsed;
  } else if (parsed && typeof parsed === 'object') {
    if (Array.isArray(parsed.deals)) deals = parsed.deals;
  }

  if (!deals) {
    alert('В тексте импорта нет списка сделок.');
    return;
  }

  const normalized = deals.filter(item => item && typeof item === 'object');
  ensureDealIds(normalized);
  savedDeals = savedDeals.concat(normalized);
  saveDealsToStorage();
  renderSavedDeals();
}

// ==== динамический расчёт max лотов ====
async function updateMaxLots() {
  const dep = parseFloat(document.getElementById('deposit').value);
  const open = parseFloat(document.getElementById('openPrice').value);
  const lev = parseFloat(document.getElementById('leverage').value);
  const pair = document.getElementById('currencyPair').value.trim().toUpperCase();
  let quote;
  try {
    ({ quote } = parseCurrencyPair(pair));
  } catch {
    document.getElementById('lots').removeAttribute('max');
    return;
  }
  const size = getContractSize(pair);

  if ([dep, open, lev].some(v => isNaN(v) || v <= 0)) {
    document.getElementById('lotsLabel').innerText = 'Количество лотов:';
    document.getElementById('lots').removeAttribute('max');
    return;
  }

  let depositInQuote = dep;
  const depositCurrency = document.getElementById('depositCurrency').value;
  if (depositCurrency !== quote) {
    try {
      const rate = await getExchangeRate(depositCurrency, quote);
      depositInQuote = dep * rate;
    } catch {
      return;
    }
  }

  const maxLots = (depositInQuote * lev) / (open * size);
  document.getElementById('lotsLabel').innerText =
    `Количество лотов (max: ${maxLots.toFixed(2)}):`;
  document.getElementById('lots').max = maxLots.toFixed(2);
}

// ==== основной расчёт ====
async function calculate(e) {
  e.preventDefault();
  saveFormValues();

  const pair = document.getElementById('currencyPair').value.trim().toUpperCase();
  const dep = parseFloat(document.getElementById('deposit').value);
  const depCur = document.getElementById('depositCurrency').value;
  const open = parseFloat(document.getElementById('openPrice').value);
  const tp = parseFloat(document.getElementById('takeProfit').value);
  const sl = parseFloat(document.getElementById('stopLoss').value);
  const lots = parseFloat(document.getElementById('lots').value);
  const lev = parseFloat(document.getElementById('leverage').value);

  if ([dep, open, tp, sl, lots, lev].some(v => isNaN(v)) || !pair) {
    alert('Заполните все поля корректно.');
    return;
  }

  let base, quote;
  try {
    ({ base, quote } = parseCurrencyPair(pair));
  } catch (err) {
    alert(err.message);
    return;
  }

  let convRate = 1;
  if (quote !== depCur) {
    try {
      convRate = await getExchangeRate(quote, depCur);
    } catch {
      return;
    }
  }

  const size = getContractSize(pair);
  const margin = (lots * size * open) / lev;

  let profitQuote, lossQuote;
  if (tp >= open) {
    profitQuote = (tp - open) * lots * size;
    lossQuote = (open - sl) * lots * size;
  } else {
    profitQuote = (open - tp) * lots * size;
    lossQuote = (sl - open) * lots * size;
  }

  const profit = profitQuote * convRate;
  const loss = lossQuote * convRate;

  const posCost = open * lots * size;
  const profitPosPct = (profitQuote / posCost) * 100;
  const lossPosPct = (lossQuote / posCost) * 100;
  const profitDepPct = (profit / dep) * 100;
  const lossDepPct = (loss / dep) * 100;

  const marginInDeposit = margin * convRate;

  document.getElementById('profitMoney').textContent =
    `Прибыль: ${Math.abs(profit.toFixed(2))} ${depCur}`;
  document.getElementById('lossMoney').textContent =
    `Убыток: ${Math.abs(loss.toFixed(2))} ${depCur}`;
  document.getElementById('marginInfo').textContent =
    `Требуемая маржа: ${marginInDeposit.toFixed(2)} ${depCur}`;
  document.getElementById('profitDepositPercent').textContent =
    `Прибыль от депозита: ${Math.abs(profitDepPct.toFixed(2))} %`;
  document.getElementById('lossDepositPercent').textContent =
    `Убыток от депозита: ${Math.abs(lossDepPct.toFixed(2))} %`;
  document.getElementById('profitPositionPercent').textContent =
    `Прибыль по позиции: ${Math.abs(profitPosPct.toFixed(2))} %`;
  document.getElementById('lossPositionPercent').textContent =
    `Убыток по позиции: ${Math.abs(lossPosPct.toFixed(2))} %`;

  lastCalculation = {
    pair,
    dep,
    depCur,
    open,
    tp,
    sl,
    lots,
    leverage: lev,
    margin: Math.abs(marginInDeposit),
    profit: Math.abs(profit),
    loss: Math.abs(loss),
    profitDepPct: Math.abs(profitDepPct),
    lossDepPct: Math.abs(lossDepPct),
    profitPosPct: Math.abs(profitPosPct),
    lossPosPct: Math.abs(lossPosPct),
    direction: tp >= open ? 'BUY' : 'SELL',
    created: Date.now()
  };

  const saveBtn = document.getElementById('saveDealBtn');
  if (saveBtn) {
    saveBtn.disabled = false;
    saveBtn.textContent = editingDealId ? 'Обновить сделку' : 'Сохранить сделку';
  }
}

// ==== смена темы при выборе актива ====
function updateAssetTheme() {
  const pair = document.getElementById('currencyPair').value;
  const box = document.getElementById('calculatorBox');
  const index = isIndexPair(pair);
  box.classList.toggle('index-selected', index);
  box.classList.toggle('crypto-selected', isCryptoPair(pair) && !index);
  box.classList.toggle('forex-selected', !isCryptoPair(pair) && !index);
}

// ==== инициализация ====
document.addEventListener('DOMContentLoaded', () => {
  loadFormValues();
  updateAssetTheme();
  updateMaxLots();

  document.querySelectorAll('#calcForm input, #calcForm select, #calcForm textarea')
    .forEach(el => el.addEventListener('input', () => {
      saveFormValues();
      if (['deposit', 'openPrice', 'leverage', 'depositCurrency'].includes(el.id)) {
        updateMaxLots();
      }
    }));

  document.getElementById('currencyPair')
    .addEventListener('change', () => {
      updateAssetTheme();
      updateMaxLots();
    });

  document.getElementById('calcForm')
    .addEventListener('submit', calculate);

  const tbody = document.getElementById('savedDealsBody');
  if (tbody) {
    // удаление сделки
    tbody.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-action][data-id]');
      if (!btn) return;
      const id = btn.getAttribute('data-id');
      const action = btn.getAttribute('data-action');
      if (action === 'delete') {
        if (confirm('Удалить сохраненную сделку?')) {
          deleteDeal(id);
        }
      } else if (action === 'edit') {
        startEditDeal(id);
      }
    });

    // онлайн-обновление комментария
    tbody.addEventListener('input', (e) => {
      const ta = e.target.closest('textarea[data-id]');
      if (!ta) return;
      const id = ta.getAttribute('data-id');
      const deal = savedDeals.find(d => String(d.id) === String(id));
      if (!deal) return;
      deal.comment = ta.value;
      saveDealsToStorage();
    });
  }

  const sortBySel = document.getElementById('sortBy');
  if (sortBySel) sortBySel.addEventListener('change', renderSavedDeals);

  const sortOrderSel = document.getElementById('sortOrder');
  if (sortOrderSel) sortOrderSel.addEventListener('change', renderSavedDeals);

  const clearBtn = document.getElementById('clearDealsBtn');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      if (!savedDeals.length) return;
      if (confirm('Удалить все сохранённые сделки?')) {
        savedDeals = [];
        saveDealsToStorage();
        renderSavedDeals();
      }
    });
  }

  const importBtn = document.getElementById('importDealsBtn');
  if (importBtn) {
    importBtn.addEventListener('click', () => {
      const input = document.getElementById('importDealsText');
      if (!input) return;
      const text = input.value.trim();
      if (!text) return;
      importDealsFromText(text);
      input.value = '';
    });
  }

  const saveBtn = document.getElementById('saveDealBtn');
  if (saveBtn) {
    saveBtn.disabled = true;
    saveBtn.addEventListener('click', () => {
      if (!lastCalculation) {
        alert('Сначала выполните расчёт сделки.');
        return;
      }
      const commentInput = document.getElementById('dealComment');
      const comment = commentInput ? commentInput.value.trim() : '';

      if (editingDealId) {
        const existing = savedDeals.find(d => String(d.id) === String(editingDealId));
        if (!existing) {
          editingDealId = null;
        } else {
          const updated = {
            ...existing,
            ...lastCalculation,
            comment,
            id: existing.id,
            created: existing.created || lastCalculation.created
          };
          savedDeals = savedDeals.map(d => String(d.id) === String(editingDealId) ? updated : d);
          saveDealsToStorage();
          renderSavedDeals();
          editingDealId = null;
          saveBtn.disabled = true;
          saveBtn.textContent = 'Сделка обновлена';
          return;
        }
      }

      const deal = {
        ...lastCalculation,
        comment,
        id: String(Date.now()) + '_' + Math.floor(Math.random() * 1000)
      };
      savedDeals.push(deal);
      saveDealsToStorage();
      renderSavedDeals();

      saveBtn.disabled = true;
      saveBtn.textContent = 'Сделка сохранена';
    });
  }

  loadSavedDealsFromStorage();
});
