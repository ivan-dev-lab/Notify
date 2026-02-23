Ниже — **ТЗ на перестройку формата хранения данных в Notify**, чтобы он стал “единой точкой истины” для Writer и для дальнейшей автоматизации планов.

---

# ТЗ: Перестройка формата хранения данных в Notify (Market State + Scenarios)

## 1) Цель

Перевести Notify с разрозненных файлов/структур на **единый формат хранения состояния рынка и сценариев** в виде агрегированного JSON, пригодного для:

* Writer (авто-обновление планов, валидация, человек-в-петле)
* дебага (почему сценарий появился/исчез)
* стабильных инкрементальных обновлений (без дублей)

---

## 2) Требования верхнего уровня

### 2.1. Единый “снимок рынка”

Для каждого инструмента хранится **один** основной файл:

* `output/State/<SYMBOL>.json`

Этот файл содержит:

* текущую цену и время обновления
* по каждому TF (`M15/H1/H4/D1/W1/MN1`) — списки элементов (FVG/SNR/Fractals) + метаданные обновления
* computed/derived контекст (опционально)
* сценарии (transition + deal) с привязками к элементам

### 2.2. Инкрементальное обновление

Notify обновляет `State/<SYMBOL>.json` **только при изменениях**, а именно:

* добавлены новые элементы,
* изменился статус существующих элементов (mitigated/invalidated/retested),
* изменился `last_bar_time`,
* изменилось множество активных сценариев.

---

## 3) Директории и файлы

### 3.1. Новые пути

* `output/State/<SYMBOL>.json` — основной файл состояния
* `output/State/schema_version.json` (опционально) — версия схемы и changelog
* `output/State/_history/<SYMBOL>/<YYYY-MM-DD>.jsonl` (опционально) — журнал изменений (event log)

### 3.2. Старые пути (на этапе миграции)

Если сейчас есть:

* `output/FVG/<SYMBOL>.json`
* `output/SNR/<SYMBOL>.json`
* `output/Fractals/<SYMBOL>.json`

то на переходный период допускается:

* генерация `State/<SYMBOL>.json` из текущих файлов,
* но итоговый источник истины должен стать **State**.

---

## 4) Схема JSON: `output/State/<SYMBOL>.json`

### 4.1. Корневые поля

```json
{
  "schema_version": "1.0.0",
  "symbol": "SPX500",
  "updated_at_utc": "2026-02-23T09:23:00+00:00",

  "market": {
    "price": 6858.40,
    "bid": 6858.30,
    "ask": 6858.50,
    "source": "MT5",
    "tick_time_utc": "2026-02-23T09:22:58+00:00"
  },

  "timeframes": { ... },

  "derived": { ... },

  "scenarios": { ... }
}
```

**Обязательные поля:**

* `schema_version`
* `symbol`
* `updated_at_utc`
* `market.price` и `market.tick_time_utc`
* `timeframes` (все 6 TF)

---

## 5) Блок `timeframes`

### 5.1. Общая структура на TF

```json
"timeframes": {
  "M15": {
    "initialized": true,
    "updated_at_utc": "...",
    "last_bar_time_utc": "...",
    "elements": {
      "fvg": [],
      "snr": [],
      "fractals": []
    }
  },
  "H1": { ... },
  ...
}
```

**Требования:**

* `elements` разделён по типам, чтобы Writer не фильтровал общий список.
* В каждом массиве элементы **стабильно идентифицируются** по `id`.

---

## 6) Форматы элементов (внутри `elements`)

### 6.1. Общие поля (для всех элементов)

Каждый элемент обязан иметь:

* `id`
* `symbol`
* `timeframe`
* `element_type`
* `formation_time_utc` (или аналогично специфике: `pivot_time_utc`)

Пример общего каркаса:

```json
{
  "id": "....",
  "element_type": "snr",
  "symbol": "SPX500",
  "timeframe": "H4",
  ...
}
```

---

### 6.2. FVG-элемент (совместимо с текущим)

Оставить максимально совместимым с тем, что у тебя уже есть, но привести имена к единообразию времени:

* `formation_time` → `formation_time_utc`
* `c1_time/c2_time/c3_time` → `*_time_utc`

Статусы: `active | touched | mitigated_full` (+ optional partial)

---

### 6.3. Fractals (3-свечные)

```json
{
  "id": "...",
  "element_type": "fractal",
  "symbol": "SPX500",
  "timeframe": "H4",

  "fractal_type": "high|low",
  "pivot_time_utc": "...",
  "confirm_time_utc": "...",

  "c1_time_utc": "...",
  "c2_time_utc": "...",
  "c3_time_utc": "...",

  "extreme_price": 6880.1,
  "l_price": 6865.57,
  "l_alt_price": 6865.6,

  "metadata": {}
}
```

---

### 6.4. SNR (зона + lifecycle)

```json
{
  "id": "...",
  "element_type": "snr",
  "symbol": "SPX500",
  "timeframe": "H4",

  "origin_fractal_id": "...",
  "role": "support|resistance",
  "break_type": "break_up_close|break_down_close",

  "break_time_utc": "...",
  "break_close": 6816.35,

  "l_price": 6865.57,

  "departure_extreme_price": 6880.10,
  "departure_extreme_time_utc": "...",
  "departure_range_start_time_utc": "...",
  "departure_range_end_time_utc": "...",

  "snr_low": 6865.57,
  "snr_high": 6880.10,

  "status": "active|retested|invalidated",
  "retest_time_utc": null,
  "invalidated_time_utc": null,

  "metadata": {}
}
```

**Ключевое требование:**
`snr_low/snr_high` **всегда** соответствуют зоне, которую нужно рисовать на графике.

---

## 7) Блок `derived` (опционально, но нужен для Writer)

Содержит вычисленные характеристики контекста, не являющиеся “сырой структурой рынка”.

Пример:

```json
"derived": {
  "htf_bias": {
    "direction": "bearish|bullish|neutral",
    "reason_ids": ["snr:...", "fvg:..."]
  },
  "global_blocks": {
    "no_long": ["snr:..."],
    "no_short": ["snr:..."]
  }
}
```

---

## 8) Блок `scenarios` (в Notify, для автопланов)

Notify формирует кандидаты сценариев. Writer их принимает/валидирует/рендерит.

### 8.1. Структура

```json
"scenarios": {
  "transition": [],
  "deals": []
}
```

### 8.2. Transition scenario

```json
{
  "id": "...",
  "symbol": "SPX500",
  "timeframe": "M15",
  "created_at_utc": "...",
  "updated_at_utc": "...",

  "state": "active|triggered|expired|invalid",

  "notation": "CREATE ...",
  "text": "Человекочитаемое описание",

  "evidence_ids": ["snr:...", "fvg:...", "fractal:..."],
  "conditions": {
    "type": "retest_snr",
    "snr_id": "...",
    "direction": "..."
  },

  "metadata": {}
}
```

### 8.3. Deal scenario

```json
{
  "id": "...",
  "symbol": "SPX500",
  "timeframe": "M15",
  "created_at_utc": "...",
  "updated_at_utc": "...",

  "state": "candidate|armed|entered|cancelled|expired",

  "direction": "long|short",
  "transition_ref": "transition:<id>",

  "entry": { "type": "limit|market", "price": 0, "zone": [a,b] },
  "sl": { "type": "price", "price": 0 },
  "tp": { "type": "price", "price": 0 },

  "constraints": {
    "forbidden_by_htf": ["snr:..."]
  },

  "evidence_ids": ["snr:...", "fvg:..."],
  "metadata": {}
}
```

---

## 10) Критерии готовности

1. Для каждого символа создаётся `output/State/<SYMBOL>.json` по схеме 1.0.0.
2. Все 6 таймфреймов присутствуют и обновляются по расписанию.
3. `snr_low/snr_high` совпадают с ожидаемой зоной на графике (по departure-логике).
4. Нет дублей элементов при многократных обновлениях (стабильные `id`).
5. Writer может обновлять план, читая только `State/<SYMBOL>.json`.

---