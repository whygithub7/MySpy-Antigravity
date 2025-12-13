# Facebook Ads Library MCP Server

MCP-сервер для поиска и анализа рекламы из Facebook Ad Library с AI-фильтрацией через Google Gemini.

## Требования

- Python 3.12+
- API ключ ScrapeCreators — https://scrapecreators.com/dashboard
- API ключ Google Gemini — https://ai.google.dev/

## Установка

```bash
# 1. Клонируй репозиторий
git clone https://github.com/YOUR_USERNAME/manual_server.git
cd manual_server

# 2. Создай виртуальное окружение
python -m venv venv

# 3. Активируй (Windows)
venv\Scripts\activate

# 4. Установи зависимости
pip install -r requirements.txt

# 5. Настрой API ключи
copy .env.example .env
# Открой .env и впиши свои ключи
```

## Настройка .env

```env
SCRAPECREATORS_API_KEY=твой_ключ_scrapecreators
GEMINI_API_KEY=твой_ключ_gemini
```


## Последний шаг: установка MCP в Antigravity

Добавь в `mcp_config.json`:

```json
{
  "mcpServers": {
    "fb_ad_library": {
      "command": "path/to/manual_server/venv/Scripts/python.exe",
      "args": [
        "path/to/manual_server/manual_mcp.py"
      ],
      "env": {
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": "path/to/manual_server",
        "USERPROFILE": "C:\\Users\\YOUR_USERNAME",
        "PATH": "C:\\Windows\\system32;C:\\Windows;C:\\Program Files\\Git\\cmd"
      }
    }
  }
}
```

## Двльшк просто скажите агенту "найди объявления по запросу prostate health и отфильтруй строго". Запроси сразу 1000 объявлений.

## Доступные инструменты

### search_ads_final
Поиск рекламы по ключевым словам.

```json
{
  "query": "prostate health",
  "country": "US",
  "limit": 1000,
  "max_ads": 1000,
  "active_status": "ACTIVE",
  "apply_filtering": true,
  "target_file": "results/ads.json"
}
```

**Параметры:**
- `query` — ключевое слово для поиска
- `country` — код страны (US, DE, RS и т.д.)
- `limit` — лимит объявлений за один запрос API
- `max_ads` — максимум объявлений для сбора
- `active_status` — ACTIVE / INACTIVE / ALL
- `apply_filtering` — включить AI-фильтрацию
- `analyze_media` — анализировать изображения/видео через Gemini
- `target_file` — путь для сохранения JSON

### get_meta_platform_id
Получить ID страниц Facebook по названию бренда.

```json
{
  "brand_names": ["Nike", "Adidas"]
}
```

### get_meta_ads_external_only
Получить рекламу только с внешними ссылками (не Facebook/Instagram).

```json
{
  "platform_ids": ["123456789"],
  "country": "US",
  "limit": 100
}
```

## Структура проекта

```
manual_server/
├── manual_mcp.py          # Точка входа MCP сервера
├── mcp_library.py         # Основная логика и инструменты
├── requirements.txt       # Python зависимости
├── .env                   # API ключи (не коммитить!)
├── .env.example           # Шаблон для .env
├── services/
│   ├── scrapecreators_service.py   # Работа с ScrapeCreators API
│   ├── gemini_service.py           # Интеграция с Google Gemini
│   └── media_cache_service.py      # Кэширование медиа
└── results/               # Папка для сохранения результатов
```

