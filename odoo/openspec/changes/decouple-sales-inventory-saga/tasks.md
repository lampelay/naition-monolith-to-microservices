# Tasks

## 1. Инфраструктура шины и движка

- [ ] 1.1 Поднять Kafka (топики `ordering.order.v1`, `inventory.promise.v1`, `purchase.po.v1`, `fulfillment.delivery.v1` + `*.dead.v1`, retention и max-partition зафиксировать в конфиге) — проверить `kafka-topics --describe` по всем списку.
- [ ] 1.2 Настроить Debezium-слот/пайплин `wal`→Kafka для таблиц `stock_quant`, `stock_move`, `stock_move_line`, `product_product`, `product_template` (publication only, без обратных записей) — проверить появлением тестового изменения таблицы в потоке.
- [ ] 1.3 Создать Temporal namespace `fulfillment` + deploy-конфиг оркестратор-воркера (без бизнес-кода) — проверить `temporal workflow list` (пустой, но API отвечает).

## 2. Transactional outbox в монолите

- [ ] 2.1 Addon-мост (раб. имя `fulfillment_bridge`): таблицы `<domain>_outbox` + `kafka_dlq`, API `publish(event_type, payload)` пишет outbox-строку в текущей транзакции — проверить unit-тестами: commit→строка цела, rollback→строки нет.
- [ ] 2.2 Relay-poller как отдельный процесс (чтение outbox → Kafka, mark-published, идемпотентно по `event_id`), wake-up через `cr.postcommit` как оптимизация — проверить crash-тестом: kill relay после commit до publish → после рестарта событие доставлено ровно один раз с тем же `event_id`.
- [ ] 2.3 Документ `docs/outbox.md` в addon: контракт envelope из спеки `domain-event-integration`, порядок развертывания relay — проверить, что пример-конверт из докам валидируется валидатором схемы envelope из 4.1.

## 3. Inventory-overlay: скелет и ATP

- [ ] 3.1 Каркас сервиса (Python, свой Postgres): конфигурация, healthcheck, consumer CDC-топика в read-model остатков (реплика quantity/reserved по (sku, warehouse)) — проверить: воспроизведение снапшота + потока даёт совпадение `free_qty` с монолитом на тестовом датасете.
- [ ] 3.2 Расчёт `free_atp = Σ quantity − Σ reserved − Σ promises − buffer`, таблица `buffer_policy(sku, value)`; planned receipts исключены из количества — проверить unit-тестами по всем трём сценариям спеки (ATP базовый, override, planned=0).
- [ ] 3.3 SLO свежести CDC: метрика лага реплики, переход в hold при лаге > SLO — проверить интеграционным тестом: заморозка потока → новые команды получают retry-отказ; hold снимается после возврата лага.

## 4. PromiseLedger: ядро обещаний

- [ ] 4.1 Валидатор envelope событий/команд (`event_id`, `type`, `schema_version`, `occurred_at`, `correlation_id`, `data`) как общий пакет — проверить round-trip тестами + отклонением битых конвертов.
- [ ] 4.2 Команда `PromiseStock`: key идемпотентности `(order_line_ref, line_version)` с версией-кортежом `(qty, route_hash, deadline)`, атомарная проверка `qty ≤ free_atp`, состояния ACTIVE/Shortage — проверить сценариями спеки (достаточно/недостаточно/повтор команды) и конкурентным тестом (два потока, сумма не превышает ATP).
- [ ] 4.3 Потребление обещаний `DeliveryValidated` (снапшоты per-move с `seq`, last-write-wins; PARTIAL с остатком ACTIVE; rounding base-UoM HALF-UP precision «Product Unit») — проверить: повторная доставка не меняет сумму; частичная отгрузка 6/10 → PARTIAL(4).
- [ ] 4.4 TTL: `expires_at` из lead-цепочек (MTS: commitment/expected − security_lead − ship_sla_margin; MTO: delay-цепочка + supplier delay + приёмочный SLA), эскалация на 80%, `PromiseExpired` с восстановлением ATP; заказ не отменяется — проверить тестами с ускоренными таймерами по обоим режимам.
- [ ] 4.5 Команды `AdjustReservation`/`ReleaseReservation`: освобождение только не потреблённого, отказ `new_qty < потреблённого`, идемпотентность по версии — проверить негативным сценарием спеки (отзыв отгруженной части) и re-доставкой команды.
- [ ] 4.6 shortage-реестр + матчинг `GoodsReceived` (FIFO по `date_planned`, частичное закрытие, `UNMATCHED_RECEIPT`) — проверить сценариями спеки (два дефицита, избыточный приход).

## 5. Watchdog, fail-closed, харакри

- [ ] 5.1 Tier-1 self-heal: детект дрейфа `Σ promises` vs ATP (порог ε1), hold-режим + пересчёт replay-ом, авто-снятие hold — проверить инъекцией дрейфа (обещание при нулевом остатке).
- [ ] 5.2 Tier-2 fail-closed: oversell сверх буфера / дрейф реплики > ε2 → отказ write-команд, `exit(1)`, startup-гейт full-reconcile до re-admit — проверить crash-тестом: SIGKILL в середине → после рестарта трафик не принимается до окончания сверки.
- [ ] 5.3 Runbook `docs/watchdog.md`: пороги, hold-метрики, процедура ручного разрешения `UNMATCHED_RECEIPT` — проверить, что команды из runbook исполнимы на dev-стенде.

## 6. Fulfillment-orchestrator (Temporal)

- [ ] 6.1 Workflow-машина состояний (`awaiting_allocation → allocated → planned → shipping → delivered → closed`, `shortage_replenishment`, `EXCEPTION_WAITING`, `SUSPENDED`), инстанс = заказ, шаг = строка — проверить workflow-тестом переходов на мок-участниках.
- [ ] 6.2 Политики retry: транспортные (exponential 1s→30s, 10 попыток) vs бизнес-отказы (non-retriable → ветка саги), durable timers 80%/timeout шага — проверить тестом Temporal TestEnvironment с ускоренным временем (таймаут шага попадает в эскалацию, не в ретрай).
- [ ] 6.3 Реакция на недоступный/fail-closed Inventory: саги → SUSPENDED без компенсаций, re-admit → повтор команд с исходными ключами — проверить тестом с обрывом Activity.
- [ ] 6.4 `picking_policy`: 'one' = wait-all до planned, 'direct' = per-line — проверить двумя workflow-тестами (частичная и полная готовность строк).

## 7. Legacy-адаптеры монолита

- [ ] 7.1 Процесс-адаптер Kafka→Odoo (внешний к воркерам; вызов штатных endpoint'ов), с poison-handling в `kafka_dlq` + алерт — проверить: poison-событие уходит в DLQ-таблицу, партиция продолжает обрабатываться.
- [ ] 7.2 Адаптер `ReplenishRequested`: создание/обновление PO текущей логикой buy-цепочки с merge, публикация `POAccepted` (строка PO, qty, date_planned) — проверить contract-тестом `purchase.po.v1` + unit-тестом merge (два заказа на один PO).
- [ ] 7.3 Компенсация `POLineDecrease` (меньшение строки PO без отмены PO; отказ при уже принятом количестве) — проверить тестами обеих веток.

## 8. Ordering: проекции и guard'ы

- [ ] 8.1 `allocation_state` на строке заказа: применение событий `PromiseCreated/Shortage/PromiseExpiring/PromiseExpired` монотонно по `seq` — проверить тестом перестановки доставки (старый seq игнорируется).
- [ ] 8.2 Проекты `qty_delivered`/`effective_date` из снапшотов `DeliveryValidated` (возвраты -=; rounding базовая Eд./HALF-UP) и производные `invoice_status` — проверить сценариями спеки (6 −2 = 4; полное закрытие при 9/10) сличением с старой формулой на общем датасете.
- [ ] 8.3 Guard уменьшения подтверждённой строки: синхронная условная `AdjustReservation` вместо проверки по локальной проекции — проверить гонкой-тестом: проекция отстаёт, сервер отклоняет по факту запаса.

## 9. Оптимистичное подтверждение под флагом

- [ ] 9.1 Feature-flag per-company `fulfillment.optimistic`: ветка confirm пишет состояние + `OrderConfirmed` в outbox и НЕ вызывает `_action_launch_stock_rule`; флаг выключен → старый путь побитово прежний — проверить регрессионными тестами `sale_stock` при выключенном флаге.
- [ ] 9.2 Ветка confirm с флагом: заказ подтверждается при недоступном Inventory (сага pending, без ошибки пользователю); shortage по нулевому остатку не блокирует confirm — проверить интеграционными тестами обеих сценариев спеки.

## 10. Интеграция и rollout-проверки

- [ ] 10.1 Shadow-метрика дрейфа: ATP-overlay vs `free_qty` монолита на боевой выборке, критерий выхода из shadow из плана миграции — проверить дашбордом/скриптом со сходимостью дрейфа < ε1 за окно.
- [ ] 10.2 Golden E2E: confirm→shortage→replenish→приход→re-promise→ship→сверка проекций и `invoice_status` — проверить прогоном на полном стенде (все участники).
- [ ] 10.3 Crash E2E: kill Inventory на шаге PromiseCreated → SUSPENDED → восстановление → ровно-один-эффект (без двойного резерва/отмены) — проверить повторным прогоном и сверкой PromiseLedger с фактами.
- [ ] 10.4 Rollback-проверка: включённый canary → выключение флага → подтверждение legacy-путём с сохранёнными данными, PromiseLedger перестроен replay-ем — проверить прогоном на стэнде до расширения на компании.
- [ ] 10.5 Пройти обязательные гейты из Reviewer Suitability Matrix (db-consistency, database-performance, runtime-performance, security-qa) по итоговым артефактам — задокументировать аппрувы в change.

## Workflow follow-up

- Архивация изменения только после закрытия задач 10.x и требований `rules/review-rules.md`.
- При синке спецификаций на архивации: поведение монолита под выключенным флагом остаётся базовым (старые модули не помечать удалёнными — деprecation вне фазы-1).
