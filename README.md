# Forex Quotes + Telegram Alerts

Скрипт получает котировки через MetaTrader 5, сохраняет их в JSON и может запускаться как Telegram-бот с инлайн-кнопками и ценовыми оповещениями.

## Структура

- `config/site_config.yaml` — настройки активов, MetaTrader и Telegram
- `src/config_loader.py` — загрузка конфигурации
- `src/main.py` — сбор котировок через MT5 и сохранение в JSON
- `src/bot.py` — Telegram-бот на aiogram (inline UI + оповещения)

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
