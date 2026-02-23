# Forex Quotes + Telegram Alerts

Скрипт получает котировки через MetaTrader 5, сохраняет их в JSON и может запускаться как Telegram-бот с инлайн-кнопками и ценовыми оповещениями.

## Структура

- `config/site_config.yaml` — настройки активов, MetaTrader и Telegram
- `src/config_loader.py` — загрузка конфигурации
- `src/main.py` — сбор котировок через MT5 и сохранение в JSON
- `src/bot.py` — Telegram-бот на aiogram (inline UI + оповещения)
- `src/auto_eye_runner.py` — запуск движка AutoEye (детекторы рыночных структур: FVG / fractal / SNR)
- `src/auto_eye/` — модульная архитектура детекторов (расширяемо под FVG и другие элементы)

## Установка

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Конфиг

Пример `config/site_config.yaml`:

```yaml
scraper:
  assets:
    - GBP/USD
    - GBP/CHF
  output_json: "output/forex_quotes.json"
  symbol_map: {}  # optional: {"GBP/USD": "GBPUSD.m"}
metatrader:
  login: 0  # optional if terminal is already logged in
  password: ""  # optional if terminal is already logged in
  server: ""  # optional if terminal is already logged in
  terminal_path: ""  # optional
  timeout_ms: 10000
telegram:
  bot_token: ""
  check_interval_seconds: 300
  alerts_json: "output/alerts.json"
logging:
  level: "INFO"
  file: "logs/notify.log"
  max_bytes: 5000000
  backup_count: 5
auto_eye:
  enabled: true
  symbols: []              # empty -> scraper.assets
  timeframes: ["M15", "H1", "H4", "D1", "W1", "MN1"]
  elements: ["fvg"]  # fvg | fractal | snr
  history_days: 30
  history_buffer_days: 5
  incremental_bars: 500
  scheduler_poll_seconds: 60
  update_interval_seconds: 300  # legacy, можно оставить
  output_json: "output/auto_eye_zones.json"
  state_json: "output/auto_eye_state.json"
  min_gap_points: 0
  require_displacement: false
  displacement_k: 1.5
  atr_period: 14
  median_body_period: 20
  fill_rule: "both"        # touch | full | both
```

Можно задавать MT5-данные через переменные окружения:

- `MT5_LOGIN`
- `MT5_PASSWORD`
- `MT5_SERVER`
- `MT5_TERMINAL_PATH`

Логи пишутся в консоль и в файл (`logging.file`) с ротацией.

## Запуск сборщика

```bash
python src/main.py
```

Сохраняет котировки в `output/forex_quotes.json`.

## Запуск Telegram-бота

```bash
python src/bot.py
```

Что умеет бот:

- показывает список котировок и текущие цены;
- управляется только инлайн-кнопками (без текстовой клавиатуры);
- имеет отдельное меню управления алертами;
- поддерживает ценовые алерты (`выше` / `ниже`);
- поддерживает алерты по времени: `через 15 минут`, `через 1 час`, `через 4 часа`, а также пользовательское время (`GMT+5`);
- проверяет условия по расписанию (`check_interval_seconds`) и по кнопке `Обновить и проверить`;
- при срабатывании отправляет сообщение в чат, после чего алерт срабатывает один раз;
- у сработавшего алерта есть инлайн-кнопка `Продлить алерт`.

## Запуск AutoEye (без связи с ботом)

Одноразовый запуск:

```bash
python src/auto_eye_runner.py --config config/site_config.yaml
```

Непрерывный режим с расписанием по TF (M15/H1/H4/D1/W1/MN1):

```bash
python src/auto_eye_runner.py --config config/site_config.yaml --loop
```

Расписание обновлений:

- `M15` — каждые 15 минут
- `H1` — каждый час
- `H4` — каждые 4 часа
- `D1` — раз в день
- `W1` — раз в неделю
- `MN1` (`M1`) — раз в месяц

Принудительный полный пересчёт истории (например после изменения правил):

```bash
python src/auto_eye_runner.py --config config/site_config.yaml --full-scan
```

Результаты:

- создаются отдельные JSON-файлы по активам в папке элемента:
  `output/FVG/<SYMBOL>.json`, `output/Fractals/<SYMBOL>.json`, `output/SNR/<SYMBOL>.json`
- в каждом файле актива хранятся данные по таймфреймам (`M15`, `H1`, `H4`, `D1`, `W1`, `MN1`)
- файл актива перезаписывается только при изменениях соответствующего таймфрейма (новые элементы или смена статуса)

## Сборка EXE (PyInstaller / auto-py-to-exe)

Перед сборкой в том же окружении установите зависимости:

```bash
pip install -r requirements.txt
pip install pyinstaller
```

Рабочая команда сборки:

```bash
pyinstaller --noconfirm --clean --onedir --console --name bot --paths src --collect-all MetaTrader5 --collect-all numpy src/bot.py
```

Для `auto-py-to-exe` укажите:

- Script: `src/bot.py`
- One Directory
- Console Based
- Additional Arguments: `--paths src --collect-all MetaTrader5 --collect-all numpy`

Если видите ошибку `ModuleNotFoundError: No module named 'numpy'`, значит `numpy` не был включен в сборку. Пересоберите с аргументом `--collect-all numpy`.
