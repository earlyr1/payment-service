# Payment Service

Микросервис асинхронного процессинга платежей: принимает платёж по HTTP, обрабатывает его через
эмулятор платёжного шлюза и сообщает результат клиенту вебхуком.

**Стек:** Python 3.14 · FastAPI + Pydantic v2 · SQLAlchemy 2 (async, asyncpg) · PostgreSQL 18 ·
RabbitMQ 4 (FastStream) · Alembic · Docker / docker compose · uv.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Примеры запросов](#примеры-запросов)
- [Архитектура](#архитектура)
- [Гарантии доставки](#гарантии-доставки)
- [Вебхуки](#вебхуки)
- [Конфигурация](#конфигурация)
- [Разработка и тесты](#разработка-и-тесты)
- [Эксплуатация](#эксплуатация)
- [Принятые решения и ограничения](#принятые-решения-и-ограничения)

## Быстрый старт

Нужны Docker и docker compose v2.

```bash
docker compose up -d --build --wait
```

Поднимутся:

| Сервис             | Назначение                                                     | Порт на хосте |
|--------------------|----------------------------------------------------------------|---------------|
| `postgres`         | PostgreSQL 18                                                  | —             |
| `rabbitmq`         | RabbitMQ 4 + management UI (логин/пароль `payments`/`payments`) | 15672         |
| `migrate`          | одноразовый `alembic upgrade head`, остальные ждут его         | —             |
| `api`              | HTTP API, Swagger UI: http://localhost:8000/docs               | 8000          |
| `outbox-relay`     | публикует события из таблицы `outbox` в RabbitMQ               | —             |
| `consumer`         | обработчик очереди `payments.new`                              | —             |
| `webhook-receiver` | демо-приёмник вебхуков (только для локальной проверки)         | 9000          |

Порты меняются переменными `API_PORT`, `WEBHOOK_RECEIVER_PORT`, `RABBITMQ_MANAGEMENT_PORT`,
например `API_PORT=8081 docker compose up -d --wait`.

Значения по умолчанию в `docker-compose.yml` подходят только для локальной разработки. Секреты
(`API_KEY`, `WEBHOOK_SIGNING_SECRET`, пароли) переопределяются через окружение или `.env`
(шаблон — `.env.example`).

Остановить и удалить данные: `docker compose down -v`.

## Примеры запросов

API-ключ по умолчанию: `dev-api-key-change-me-0123456789`.

```bash
export API=http://localhost:8000/api/v1/payments
export KEY=dev-api-key-change-me-0123456789
```

### Создание платежа

```bash
curl -i -X POST "$API" \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: 6f9c2b1e-order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:9000/webhooks/ok"
      }'
```

```http
HTTP/1.1 202 Accepted
location: /api/v1/payments/01a13c94-0362-7540-bd1d-878d939ffb54

{"payment_id":"01a13c94-0362-7540-bd1d-878d939ffb54","status":"pending","created_at":"2026-10-14T22:41:35.596879Z"}
```

Поля тела:

| Поле          | Тип                    | Обязательное | Ограничения                                         |
|---------------|------------------------|--------------|-----------------------------------------------------|
| `amount`      | строка или число       | да           | > 0, не более 2 знаков после запятой, до 16 цифр в целой части |
| `currency`    | `RUB` \| `USD` \| `EUR` | да           |                                                     |
| `description` | строка                 | нет          | 1–1000 символов                                     |
| `metadata`    | JSON-объект            | нет          | до 8 КБ в сериализованном виде                      |
| `webhook_url` | URL (http/https)       | да           | до 2048 символов                                    |

Неизвестные поля и символ NUL в строках (его не хранит PostgreSQL) отклоняются (422). Суммы в ответах всегда строки (`"1500.00"`), чтобы не терять
точность на float.

### Повтор запроса (идемпотентность)

Тот же `Idempotency-Key` с тем же телом вернёт исходный платёж (новый не создаётся) и заголовок
`Idempotent-Replayed: true`. Тот же ключ с другим телом — `422`:

```bash
curl -s -X POST "$API" -H "X-API-Key: $KEY" -H "Idempotency-Key: 6f9c2b1e-order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1.00", "currency": "RUB", "webhook_url": "http://webhook-receiver:9000/webhooks/ok"}'
# {"detail":"Idempotency-Key has already been used with different request parameters"}
```

### Получение платежа

```bash
curl -s "$API/01a13c94-0362-7540-bd1d-878d939ffb54" -H "X-API-Key: $KEY"
```

```json
{
  "payment_id": "01a13c94-0362-7540-bd1d-878d939ffb54",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "webhook_url": "http://webhook-receiver:9000/webhooks/ok",
  "idempotency_key": "6f9c2b1e-order-42",
  "failure_reason": null,
  "created_at": "2026-10-14T22:41:35.596879Z",
  "processed_at": "2026-10-14T22:41:38.080992Z"
}
```

`status`: `pending` → `succeeded` | `failed` (у `failed` заполнен `failure_reason`).

### Коды ответов

| Код | Когда                                                              |
|-----|--------------------------------------------------------------------|
| 202 | платёж принят (или возвращён ранее созданный по тому же ключу)     |
| 200 | GET: платёж найден                                                 |
| 400 | нет `Idempotency-Key` или он некорректен (1–255 видимых ASCII-символов) |
| 401 | нет или неверный `X-API-Key`                                       |
| 404 | платёж не найден                                                   |
| 422 | невалидное тело / `payment_id`, либо ключ идемпотентности переиспользован с другим телом |

### Посмотреть ретраи и DLQ вживую

Демо-приёмник умеет три режима (путь в `webhook_url`):

- `/webhooks/ok` — всегда 200;
- `/webhooks/flaky` — 500 на первые две доставки, потом 200: видно ретраи с задержками 2 с и 4 с;
- `/webhooks/fail` — всегда 500: после 3 попыток сообщение уходит в `payments.new.dlq`.

```bash
curl -s localhost:9000/webhooks | jq   # что получил приёмник (+ результат проверки подписи)
docker compose logs -f consumer         # ретраи и перемещение в DLQ
```

DLQ — в management UI: http://localhost:15672/#/queues → `payments.new.dlq` (Get messages).

## Архитектура

```
             POST /payments                                     ┌──────────────────────────┐
  client ───────────────────► api ── 1 transaction ───────────► │ PostgreSQL               │
                               │    INSERT payments + outbox    │  payments  ·  outbox     │
                               │                                └──────────────────────────┘
                               ▼ 202                                ▲ poll (SKIP LOCKED)  ▲
                                                                    │                     │ status,
                                                              outbox-relay                │ webhook state
                                                                    │ publish + confirm   │
                                                                    ▼                     │
        ┌───────────────────────────────── RabbitMQ ─────────────────────────────┐        │
        │ [payments] ─► (payments.new) ──────────────────────────────────────────┼─► consumer ─► gateway (emulator)
        │      ▲                                                                  │        │
        │      └── TTL ── (payments.new.retry.<N>ms) ◄── [payments.retry] ◄───────┼── retry│
        │                                                                         │        │
        │                (payments.new.dlq) ◄── [payments.dlx] ◄──────────────────┼── DLQ │
        └─────────────────────────────────────────────────────────────────────────┘        ▼
                                                                                   POST webhook_url
```

Один образ, разные роли (`python -m payment_service.api | .outbox | .consumer`):

- **api** — валидация, аутентификация, идемпотентность; в одной транзакции пишет платёж и событие
  `payment.created` в `outbox`. От брокера не зависит: при недоступном RabbitMQ продолжает принимать
  платежи (проверено).
- **outbox-relay** — забирает неопубликованные события (`FOR UPDATE SKIP LOCKED`, можно запускать
  несколько экземпляров), публикует с publisher confirms и `mandatory`, помечает `published_at`.
  Ошибки публикации копятся в `publish_attempts`/`last_error`, relay уходит в экспоненциальный backoff.
  Старые опубликованные события периодически удаляются (`OUTBOX_RETENTION_HOURS`).
- **consumer** — один обработчик `payments.new`: эмулирует шлюз (2–5 с, 90% успех), атомарно
  переводит статус, отправляет подписанный вебхук, при ошибке — ретрай через delay-очередь, после
  исчерпания попыток — DLQ. Сообщения обрабатываются конкурентно (до `CONSUMER_PREFETCH_COUNT`),
  консьюмер масштабируется горизонтально (`docker compose up -d --scale consumer=3`).

### Топология RabbitMQ

Все очереди — **quorum** (реплицируемые, durable; рекомендуемый тип начиная с RabbitMQ 4).

| Объект                         | Тип              | Назначение                                                                 |
|--------------------------------|------------------|----------------------------------------------------------------------------|
| `payments`                     | direct exchange  | основной                                                                   |
| `payments.new`                 | quorum queue     | новые платежи; DLX → `payments.dlx`, `x-delivery-limit` как страховка от crash-loop |
| `payments.retry`               | direct exchange  | ретраи                                                                     |
| `payments.new.retry.<N>ms`     | quorum queue     | по одной на задержку; `x-message-ttl=N`, по истечении TTL dead-letter обратно в `payments` |
| `payments.dlx`                 | direct exchange  | dead letters                                                               |
| `payments.new.dlq`             | quorum queue     | окончательно не обработанные сообщения                                     |

Топологию идемпотентно объявляют и relay, и consumer при старте — порядок запуска не важен, и
сообщение не может уйти «в никуда» (а если вдруг уйдёт — `mandatory` + `on_return_raises` превратят
это в ошибку публикации, и событие останется в outbox).

Почему по очереди на каждую задержку, а не TTL на сообщении: RabbitMQ истекает сообщения только
с головы очереди, поэтому при per-message TTL короткая задержка застрянет за длинной. Задержка
входит в имя очереди, так что изменение конфигурации создаёт новую очередь, а не падает с
`PRECONDITION_FAILED` при повторном объявлении.

### Retry и DLQ

- Попытка = одна обработка сообщения консьюмером. Номер попытки — в заголовке `x-attempt`.
- `CONSUMER_MAX_ATTEMPTS=3` попытки всего; задержка перед попыткой n+1 = `base · 2^(n-1)`
  (по умолчанию 2 с, 4 с; ограничена `CONSUMER_RETRY_MAX_DELAY_SECONDS`).
- Ретраятся технические ошибки: недоставленный вебхук (сеть, таймаут, любой не-2xx), ошибки БД,
  неизвестные исключения.
- Сразу в DLQ (без ретраев): невалидное сообщение, несуществующий платёж, вебхук на приватный адрес.
- Если этот же платёж прямо сейчас обрабатывает другая доставка (дубль), сообщение откладывается
  на минимальную задержку **без** расхода попытки (счётчик `x-postponed`, максимум 30).
- В DLQ сообщение попадает с заголовками `x-attempt`, `x-last-error`, `x-last-error-type`,
  `x-dead-lettered-at`.
- Отказ шлюза (те самые 10%) — **не ошибка обработки**, а бизнес-результат: платёж получает статус
  `failed`, клиенту уходит вебхук `payment.failed`.
- Сообщение подтверждается (ack) только когда его судьба решена: обработано, либо переопубликовано
  в retry/DLQ с подтверждением брокера. Если переопубликовать не удалось — `nack(requeue)`; от
  бесконечного цикла защищает `x-delivery-limit` quorum-очереди (брокер сам отправит в DLX).

## Гарантии доставки

Публикация событий и обработка — **at-least-once**, а обработчик идемпотентен, поэтому эффект —
exactly-once там, где это важно (списание, смена статуса).

| Сценарий отказа                                                | Что происходит                                                              |
|----------------------------------------------------------------|-----------------------------------------------------------------------------|
| RabbitMQ недоступен при создании платежа                       | API отвечает 202; событие лежит в `outbox`, relay опубликует после восстановления |
| relay упал после publish, но до отметки `published_at`         | событие опубликуется повторно; дубль безопасен (см. ниже)                   |
| consumer упал во время обработки                               | сообщение не подтверждено → брокер передоставит его другому консьюмеру      |
| две доставки одного платежа обрабатываются одновременно        | per-payment advisory lock: вторая откладывается через retry-очередь (не расходуя попытку, заголовок `x-postponed`, не более 30 раз) и потом видит готовый результат |
| crash между ответом шлюза и записью статуса                    | при повторе шлюз вызывается с тем же `payment_id` как ключом идемпотентности шлюза |
| вебхук уже доставлен, сообщение пришло снова                   | `webhook_delivered_at` заполнен → повторной отправки нет                    |
| два одинаковых запроса с одним `Idempotency-Key` одновременно  | `INSERT … ON CONFLICT DO NOTHING` по уникальному индексу: создаётся ровно один платёж и одно событие |

Подробнее про обработчик (`services/processing.py`):

1. берётся неблокирующий session-level advisory lock на платёж (на отдельном соединении; транзакция
   не держится открытой, пока ждём внешний шлюз; при смерти процесса Postgres освобождает lock сам);
2. если платёж `pending` — вызов шлюза, затем `UPDATE … WHERE status = 'pending'` (compare-and-set);
3. если вебхук ещё не доставлен — отправка; результат попытки пишется в `webhook_attempts`,
   `webhook_last_error`, `webhook_delivered_at`.

Ключ идемпотентности API хранится вместе с платежом (уникальный индекс) и не протухает; вместе с
ним хранится SHA-256 канонизированного тела запроса — так обнаруживается переиспользование ключа с
другими параметрами (`"100"` и `"100.00"` считаются одной суммой).

## Вебхуки

```http
POST <webhook_url>
Content-Type: application/json
User-Agent: payment-service-webhooks/1.0
X-Webhook-Id: 3a3c6c1e-5d0f-5b5e-9a77-0e1f2b3c4d5e
X-Webhook-Event: payment.succeeded
X-Webhook-Signature: t=1792017698,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd

{
  "id": "3a3c6c1e-5d0f-5b5e-9a77-0e1f2b3c4d5e",
  "type": "payment.succeeded",
  "created_at": "2026-10-14T22:41:38.080992+00:00",
  "data": {
    "payment_id": "01a13c94-0362-7540-bd1d-878d939ffb54",
    "status": "succeeded",
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "failure_reason": null,
    "idempotency_key": "6f9c2b1e-order-42",
    "created_at": "2026-10-14T22:41:35.596879+00:00",
    "processed_at": "2026-10-14T22:41:38.080992+00:00"
  }
}
```

- Успех — любой ответ 2xx; редиректы не выполняются (и считаются ошибкой).
- `WEBHOOK_TIMEOUT_SECONDS` — общий дедлайн на весь запрос (а не только на отдельные операции чтения),
  поэтому медленный получатель не может надолго занять воркер.
- `id` детерминирован для пары (платёж, итоговый статус) и одинаков во всех повторах — получатель
  должен дедуплицировать по нему (доставка at-least-once).
- Подпись: `v1 = hex(HMAC-SHA256(WEBHOOK_SIGNING_SECRET, "<t>." + сырое тело))`. Получатель
  пересчитывает HMAC по сырому телу, сравнивает за константное время и отклоняет `t` старше 5 минут
  (защита от replay). Готовая проверка: `payment_service.webhooks.signing.verify_signature_header`.
- SSRF-защита: по умолчанию вебхуки на loopback/private/link-local/reserved адреса запрещены
  (`WEBHOOK_ALLOW_PRIVATE_TARGETS=false`). В compose включено, чтобы работал демо-приёмник.

## Конфигурация

Всё настраивается переменными окружения (`src/payment_service/config.py`, шаблон — `.env.example`).
Обязательные: `DATABASE_URL` (`postgresql+asyncpg://…`), `RABBITMQ_URL`, `API_KEY` (≥ 16 символов),
`WEBHOOK_SIGNING_SECRET` (≥ 16 символов). Конфигурация валидируется при старте — сервис с неверными
настройками не запустится.

| Переменная                          | По умолчанию | Описание                                                   |
|-------------------------------------|--------------|------------------------------------------------------------|
| `LOG_LEVEL` / `LOG_FORMAT`          | `INFO` / `json` | уровень; `json` (по строке на событие) или `console`    |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW`  | 10 / 10      | пул соединений; для консьюмера сумма ≥ 2 × prefetch         |
| `DB_STATEMENT_TIMEOUT_MS`           | 10000        | `statement_timeout` для всех запросов                      |
| `CONSUMER_PREFETCH_COUNT`           | 10           | сколько платежей один консьюмер обрабатывает параллельно    |
| `CONSUMER_MAX_ATTEMPTS`             | 3            | попыток всего до DLQ                                        |
| `CONSUMER_RETRY_BASE_DELAY_SECONDS` | 2            | база экспоненциальной задержки                              |
| `CONSUMER_RETRY_MAX_DELAY_SECONDS`  | 300          | потолок задержки                                            |
| `CONSUMER_DELIVERY_LIMIT`           | 10           | лимит передоставок quorum-очереди (страховка от crash-loop) |
| `GATEWAY_MIN/MAX_DELAY_SECONDS`     | 2 / 5        | задержка эмулятора шлюза                                    |
| `GATEWAY_SUCCESS_RATE`              | 0.9          | доля успешных списаний                                      |
| `WEBHOOK_TIMEOUT_SECONDS`           | 10           | таймаут HTTP-запроса вебхука                                |
| `WEBHOOK_ALLOW_PRIVATE_TARGETS`     | `false`      | разрешить вебхуки на приватные адреса (только для dev)      |
| `OUTBOX_POLL_INTERVAL_SECONDS`      | 0.5          | период опроса outbox                                        |
| `OUTBOX_BATCH_SIZE`                 | 100          | событий за итерацию                                         |
| `OUTBOX_RETENTION_HOURS`            | 72           | сколько хранить опубликованные события                      |
| `API_DOCS_ENABLED`                  | `true`       | Swagger UI и `/openapi.json`                                |

## Разработка и тесты

Короткие команды есть в `Makefile` (`make up`, `make lint`, `make test`). CI — `.github/workflows/ci.yml`.

```bash
uv sync                          # окружение (Python 3.14 uv поставит сам)
uv run ruff check . && uv run ruff format --check .
uv run mypy                      # strict
uv run pytest                    # нужен Docker: Postgres и RabbitMQ поднимаются через testcontainers
uv run pytest tests/unit         # только юнит-тесты, без Docker
```

Интеграционные тесты работают с настоящими PostgreSQL 18 и RabbitMQ 4 (никаких моков брокера и БД):

- API: создание/получение, идемпотентность (включая 10 параллельных запросов с одним ключом),
  конфликт ключа, аутентификация, валидация, health, request id;
- outbox relay: публикация (message id = id события), отказ брокера (событие остаётся с ошибкой),
  очистка старых событий;
- consumer end-to-end (relay → RabbitMQ → consumer → Postgres + реальный HTTP-приёмник): успех,
  отказ шлюза, вебхук доставлен с 3-й попытки, DLQ после 3 попыток с заголовками ошибки,
  конкурентные дубли (шлюз вызывается ровно один раз, в том числе когда обработка дольше всех
  ретраев), битые сообщения сразу в DLQ;
- advisory lock: эксклюзивность и освобождение при исключении.

Новая миграция: `uv run alembic revision --autogenerate -m "..."` (нужен `DATABASE_URL`), затем
ревью сгенерированного файла; `uv run alembic check` подтверждает, что модели и миграции совпадают.

### Структура

```
src/payment_service/
  api/            FastAPI: приложение, роуты, схемы, зависимости (auth, идемпотентность), middleware
  services/       бизнес-логика: создание платежа (+outbox), обработка платежа
  repositories/   доступ к данным (payments, outbox)
  db/             модели, сессии, advisory lock
  domain/         перечисления и контракты сообщений
  messaging/      брокер и топология RabbitMQ, retry-политика
  outbox/         outbox relay (python -m payment_service.outbox)
  consumer/       FastStream-приложение и обработчик (python -m payment_service.consumer)
  gateway/        интерфейс платёжного шлюза и эмулятор
  webhooks/       отправка и подпись вебхуков
  devtools/       демо-приёмник вебхуков (не часть сервиса)
migrations/       Alembic
tests/unit, tests/integration
```

## Эксплуатация

- **Health:** `GET /health/live` (процесс жив), `GET /health/ready` (БД доступна). Без аутентификации —
  пробы оркестратора ходят без ключа и ничего не раскрывают. Брокер в readiness API намеренно не
  проверяется: благодаря outbox API работает и без него.
- **Логи:** JSON по строке на событие; в каждой записи обработки есть `message_id`, `payment_id`,
  `attempt`, у HTTP-запросов — `request_id` (берётся из `X-Request-ID` или генерируется и
  возвращается в ответе).
- **Graceful shutdown:** консьюмер по SIGTERM перестаёт брать новые сообщения и дожидается текущих
  (до 30 с; `stop_grace_period: 45s` в compose), relay завершает текущую итерацию, API — uvicorn graceful.
- **Миграции** выполняются отдельным шагом (`migrate`) до старта приложений и защищены advisory
  lock'ом от параллельного запуска несколькими репликами.
- **DLQ:** сообщения можно посмотреть в management UI (Get messages, заголовки `x-last-error*`).
  После устранения причины их можно вернуть в обработку, переместив в exchange `payments` с routing
  key `payments.new` (например, shovel или «Move messages» в UI). Повторная обработка безопасна:
  оплаченный платёж не будет списан повторно, доставленный вебхук не уйдёт второй раз.
- **Мониторинг:** что стоит алертить — глубина `payments.new.dlq` > 0, возраст самого старого
  неопубликованного события в `outbox`, растущий `publish_attempts`, лаг `payments.new`.

## Принятые решения и ограничения

- **Отдельный процесс outbox-relay** (а не фоновая задача в API): масштабируется и падает независимо,
  API не держит соединение с брокером. В compose он есть в дополнение к перечисленным в задании сервисам.
- **Повторы через брокер, а не `sleep` в обработчике:** консьюмер не держит сообщение и слот
  обработки во время ожидания, задержки переживают рестарт.
- **4xx от получателя вебхука тоже ретраятся** — получатели часто временно отвечают 404/400 во время
  деплоя; практика большинства платёжных систем.
- **Статусы** — ровно три из задания (`pending`, `succeeded`, `failed`). Состояние доставки вебхука
  хранится отдельными полями и в публичный ответ не входит.
- **UUIDv7** для идентификаторов — монотонно растут во времени, хорошо ложатся в B-tree индекс.
- **Деньги** — `NUMERIC(18,2)` и `Decimal`; в JSON — строки.
- **Advisory lock** хранится на отдельном соединении на время обработки, поэтому консьюмер требует
  `DB_POOL_SIZE + DB_MAX_OVERFLOW ≥ 2 × CONSUMER_PREFETCH_COUNT` и проверяет это при старте.
- **SSRF:** адрес проверяется резолвом перед отправкой, но HTTP-клиент резолвит имя повторно, так
  что DNS-rebinding теоретически возможен. В проде вебхуки стоит пускать через egress-прокси с той
  же политикой (например, smokescreen).
- **Один статический API-ключ** — как в задании. Для ротации без даунтайма следующим шагом стоит
  поддержать список ключей.
- **Не сделано (вне объёма задания):** метрики Prometheus и трейсинг, rate limiting, ручка для
  повторной отправки вебхука, автоматический replay DLQ.
