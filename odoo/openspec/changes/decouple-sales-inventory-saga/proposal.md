# Proposal

## Why

Домены «Продажи» (Ordering, модули `sale`/`sale_stock`) и «Запасы» (Inventory, модуль `stock`) в монолите Odoo связаны единой Postgres-транзакцией на HTTP-запрос: подтверждение заказа, изменение количества, отмена и валидация отгрузки атомарно пишут таблицы обоих доменов (`sale_order*` + `stock_move*`/`stock_quant`), включая создание строк заказа кодом стока (`sale_stock/models/stock.py:302-347`) и синхронную цепочку закупки из продажи (`purchase_stock/models/stock_rule.py:59`). Эта атомарность блокирует независимую эволюцию и масштабирование домена запасов и требует замены синхронных кросс-доменных транзакций распределёнными паттернами (Saga + Transactional Outbox) до начала strangler-миграции.

## What Changes

Фаза-1 strangler'а: «Inventory» выделяется как первый greenfield-сервис (Python, отдельный Postgres) в форме overlay — он владеет реестром обещаний (PromiseLedger) и ATP read-model, но НЕ владеет пока физическим исполнением (кванты/move'ы остаются в монолите, состояние реплицируется через CDC).

- Новая: сервис **inventory-overlay** — PromiseLedger (soft-reservation с TTL), расчёт `free_atp = Σ quantity − Σ reserved − Σ active promises − buffer`, `buffer_policy` (точечный override `sku → число`, дефолт 0), two-tier watchdog с fail-closed и самоуничтожением (harakiri).
- Новая: сервис **fulfillment-orchestrator** (standalone, Python + Temporal) — оркестрируемая сага исполнения заказа; durable timers на TTL-эскалацию.
- Новая: **transactional outbox в монолите** — таблица `*_outbox` + relay-poller в Kafka (не postcommit-публикация); Kafka — внешняя шина событий/команд; CDC (Debezium) — односторонняя репликация `stock_*`/`product` в inventory-overlay.
- **BREAKING**: подтверждение заказа (`sale.order.action_confirm`) становится оптимистичным: заказ подтверждается без синхронного создания `stock.move`/резервирования квантов в той же транзакции; исход выделения приходит событиями (`PromiseCreated`/`Shortage`). Исчезает синхронная ошибка «нет на складе» в момент confirm.
- **BREAKING**: `sale.order.line.qty_delivered` / `effective_date` / `invoice_status` становятся событийными проекциями из `DeliveryValidated`, а не compute по FK `stock.move.sale_line_id`.
- Новая: replenishment child-saga (только складская `buy`-цепочка): `Shortage → ReplenishRequested → (PO в монолите) → GoodsReceived → re-Promise`; компенсация — `POLineDecrease` (не cancel PO, т.к. PO merges между заказами).
- Новая: shortage/TTL-exception-ветка: заказ НЕ отменяется при нехватке или истечении обещания — долгий статус `EXCEPTION_WAITING` для ручного разрешения.
- Отложено явным решением: dropship, manufacturing (`_run_produce`), inter-company, канареечные чтения и перенос исполнения picking.

## Capabilities

### New Capabilities
- `inventory-promises`: реестр обещаний, расчёт ATP, buffer-политика, TTL/освобождение, watchdog и fail-closed поведение сервиса запасов.
- `order-fulfillment-saga`: машина состояний исполнения заказа — оптимистичное подтверждение, выделение, shortage/replenishment, exception-ветки, гарантии компенсации и идемпотентность шагов.
- `domain-event-integration`: контракт кросс-доменного взаимодействия — outbox в монолите, топики/конверты событий, ключи идемпотентности, CDC-репликация, contract-тесты.

### Modified Capabilities
- (нет — durable-спеков в проекте пока нет; поведение монолита после фазы-1 зафиксируется синком спецификаций при архивации)

## Impact

- **Код монолита (Odoo)**: `sale`, `sale_stock` (переход confirm/write/cancel на события, проекции `qty_delivered`), `stock` (публикация `DeliveryValidated`/`GoodsReceived` вместо прямых записей в `sale_order_line`), `purchase_stock` (адаптер `ReplenishRequested`); новый addon-мост (`*_outbox` + relay + Kafka-клиент).
- **Новые сервисы**: `inventory-overlay` (FastAPI-подобный сервис + Postgres-схема PromiseLedger/ATP/buffer_policy/watchdog), `fulfillment-orchestrator` (Temporal workflows + workers).
- **Инфраструктура**: Kafka (топики доменов + DLQ), Debezium/CDC, Temporal server + свой stateful-бэкенд, relay-poller.
- **Данные**: запрет кросс-доменных JOIN и FK `stock.move.sale_line_id` в новых путях; identifier-only ссылки (`order_line_ref` UUID + correlation), сверочные job'ы вместо отката общей транзакции.
- **Смежные модули (не в фазе-1)**: `mrp`, `delivery`, `stock_barcode`, `sale_*/account` continue по старым таблицам; никаких прямых правок их схем.
- **Риски**: oversell в окне рассинхрона CDC (принятая бизнес-семантика), гонка guard'а уменьшения количества ниже `qty_delivered` (см. design D5), удвоение резервов при неверной компенсации на зависшем узле (mitigated: SUSPENDED-not-compensate, D8).

## Reviewer Suitability Matrix

Основано на аудите связности (dependency-audit): импорты — `sale_stock` переопределяет модели обоих доменов (`_inherit`), БД — кросс-доменные FK `stock.move.sale_line_id`, `stock.picking.sale_id`, JOIN в compute'ах (`_compute_sql_order_partner_id`), транзакции — T1–T5 затрагивают таблицы `sale`+`stock`(+`purchase`) в одном курсоре; атомарность нарушается сплитом → требуются Saga + Outbox (выбрано).

| Reviewer | Статус | Обоснование |
|---|---|---|
| db-consistency-reviewer | **Выбран** | Обязательный гейт (review-rules §3): сага-state-машины, outbox-таблицы, PromiseLedger, reconcile-инварианты `Σpromises ≤ free_qty`; сверка схем саги с БД. |
| database-performance-reviewer | **Выбран** | Новые query-paths: ATP-агрегаты по реплицированным quant/move, FIFO-матчинг `GoodsReceived`, dedup-таблицы; статическая проверка индексов обязательна (review-rules §3). |
| runtime-performance-reviewer | **Выбран** | CPU-heavy (watchdog-reconcile, recompute ATP, CDC-replay) выносится из потоков запросов в фоновые консьюмеры/воркеры; ретраи Temporal и latency-контракт confirm-пути. |
| security-qa-reviewer | **Выбран** | Обязательный гейт (review-rules §3): CDC и событийные contract-тесты (golden E2E + crash-сценарий), права/фильтрация company в новых сервисах. |
| gateway-routing-reviewer | **Не релевантен** | Фаза-1 не меняет HTTP-маршрутизацию/зеркалирование трафика: вход в монологи/сервисы остаётся прежним, канареечные чтения и traffic-migration — фаза-N (отложено решением по scope). |
| Динамически summoned эксперты | Нет | Домены reviewers полностью покрывают стек (Python/Kafka/Temporal покрываются db-consistency + runtime-performance); отдельных конфигураций `<custom-reviewer>.md` не требуется. |
