# Design

## Context

Монолит Odoo 20 исполняет один HTTP-запрос = один курсор = одна Postgres-транзакция (`odoo/sql_db.py:554`), поэтому связка продаж и запасов атомарна «по построению». Аудит связности (см. proposal.md - Why; подробности T1–T6 и карту FK — в Reviewer Suitability Matrix):

- **FK/JOIN-связка**: `stock.move.sale_line_id` (`sale_stock/models/stock.py:17`), `stock.picking.sale_id` (stored-compute, там же :262), SQL-JOIN стока на `sale_order_line` (`_compute_sql_order_partner_id`), строки заказа, создаваемые кодом стока при валидации отгрузки (`StockPicking._action_done`, там же :302-347), синхронная закупка из продажи (`purchase_stock/models/stock_rule.py:59`, PO сливаются между заказами).
- **Compute-связка**: `qty_delivered/invoice_status/effective_date` заказа — stored-проекции поверх `stock.move` (`sale_stock/models/sale_order_line.py:181-210`), рекомпилируются тем же курсором.
- **Ограничения проекта** (`rules/extraction-rules.md`): изолированные схемы/БД, запрет кросс-доменных JOIN, запрет 2PC, Saga + Transactional Outbox обязательны; при откате strangler'а — синхронизация данных микросервиса обратно.

Подтверждённые участниками решения (контекст диалога): рантайм = strangler-фаза-1 (монолит остаётся исполнителем складских операций), Python + Kafka, оркестратор = standalone Temporal, подтверждения оптимистичные, обещания = soft с TTL, replenishment только складская buy-цепочка, inter-company/dropship/mrp вне фазы-1.

## Goals / Non-Goals

**Goals:**
- Убрать из общих транзакций все кросс-доменные записи Ordering↔Inventory↔Purchase(PO) фазы-1, заменив их сагой с outbox-событиями.
- Вывести PromiseLedger и ATP в отдельный сервис с локальной БД, не трогая физическое исполнение складских операций.
- Сохранить наблюдаемые инварианты (no-oversell внутри сервиса запасов; запрет отгрузки больше заказанного) локальными ACID-гарантиями одного сервиса.
- Обеспечить откат фазы-1 выключением флага без потери данных.

**Non-Goals:**
- Перенос исполнения picking/quant в сервис (фаза-N, после канареек).
- Dropship, manufacturing, inter-company (отложены), канареечные чтения и gateway-роутинг (не фаза-1).
- Изменение биллинга: `account` остаётся в монолите; меняются только источники его полей-проекций (`qty_delivered`).
- ATP с прогнозными поступлениями как количеством (forecast участвует только в расчёте сроков).

## Decisions

### D0. Граница фазы-1: inventory-overlay вместо переноса стока
Сервис запасов владеет **только** PromiseLedger + ATP read-model; кванты/move/picking остаются в монолите, их состояние приходит односторонним CDC.
*Почему:* PromiseLedger в коде Odoo отсутствует — его негде «вырезать», а перенос квантов = big-bang с dual-write всех складских потоков. *Альтернатива:* полный перенос `stock` — отклонена выбранным режимом A (strangler). *Бонус:* rollback-синхронизация из `rules/extraction-rules.md` §3 вырождается в тривиальную — физические данные не уходили из монолита, реестр обещаний отбрасывается и перестраивается replay-ем.

### D1. Безопасный буфер ATP: дефолт 0, точечный override per-SKU *(подтверждено)*
`buffer_policy` — таблица `sku → число`; категорийные/ценовые правила отложены. Планируемые поступления НЕ входят в количество ATP (`free_atp = Σ quantity − Σ reserved − Σ promises − buffer`).
*Почему:* окно oversell'а = лаг CDC; owner товара платит за него нулём, а система — усложнением формулы. *Альтернатива:* динамический буфер из orderpoint'ов — связывает promise-механику с чужим планировщиком, отклонено для фазы-1.

### D2. TTL обещания из существующих lead-цепочек *(дефолт)*
MTS: `expires_at = (commitment_date || expected_date) − security_lead − ship_sla_margin`, где `expected_date` наследует семантику `_select_expected_date` (`sale_stock/models/sale_order.py:117-120`: 'direct'→min по линиям, 'one'→max).
MTO/replenish: `ttl = цепочка delay правил (_get_lead_days, stock_rule.py:388) + supplier delay (уже двигает po.date_order в _run_buy) + приёмочный SLA + margin`. Эскалация `PromiseExpiring` на 80% TTL.
*Почему:* deadline обещания уже существует в данных (`move.date_deadline`, `customer_lead`) — переносим как первоклассное поле promise, не изобретая новый календарь. *Альтернатива:* плоский TTL конфигом — врёт пользователю в сроках, отклонена. *Нюанс:* `security_lead` в Odoo — про раннее планирование; его использование как «запаса» — семантическое приближение, помечено в Risks.

### D3. Идемпотентность и correlation *(дефолт)*
Инстанс саги = заказ (не строка — иначе не выразить picking_policy='one'); шаги гранулярны строкам. Ключ `PromiseStock = (order_line_ref, line_version)`; `line_version` — кортеж `(qty_ordered, route_hash, deadline)`, а не счётчик: забытый инкремент в legacy-коде детектируется несходством payload, а не молча сносится. События: `event_id` UUIDv7 + монотонный `seq` на источник фактов; dedup-таблица потребителя с TTL. Повтор команды через Temporal даёт ровно-один-эффект на уровне движка (Activity IdempotencyKey).

### D4. События: JSON-envelope без реестра в фазе-1 *(дефолт)*
Обязательный envelope (`event_id, type, schema_version, occurred_at, correlation_id, data`), топики по агрегату с версией (`ordering.order.v1`, `inventory.promise.v1`, `purchase.po.v1`, `fulfillment.delivery.v1`), партиционирование по ключу агрегата, DLQ `*.dead.v1` после N попыток. Schema registry (Avro) — при появлении внешних потребителей: реестр раньше времени жизни схемы = ceremony для системы из трёх участников.

### D5. Проекция отгрузок: снапшоты per-move, guard через запас *(дефолт)*
`DeliveryValidated` несёт снапшот `{move_ref, qty_in_base_uom, state, seq}` (не дельту); проекция `qty_delivered` пересчитывается идемпотентно last-write-wins per move_ref. Единицы/округление — базовая Eд. товара, HALF-UP, precision «Product Unit» — воспроизведение текущего контракта `_prepare_qty_delivered` (`sale_stock/models/sale_order_line.py:185-210`), иначе разойдёмся с документацией по счетам. Guard «нельзя уменьшить ниже доставленного» (сейчас `_update_line_quantity`, там же :418-423) при eventual-проекции основан на устаревших данных → синхронная условная `AdjustReservation(..., require_max = потреблённое из PromiseLedger)`; UI-guard остаётся эвристикой, финальная проверка — в сервисе запасов.

### D6. Матчинг прихода: FIFO shortage-реестра + UNMATCHED_RECEIPT *(дефолт)*
`GoodsReceived` привязан к PO-строке (merge'ит `_run_buy`), а не к заказу; распределение прихода по shortage — FIFO по `date_planned` Inventory-сервиса (зеркалит сегодняшнюю очередь резервирования `_gather`/FEFO по `in_date`, `stock_quant.py:803`). Избыток прихода без покрытия → исключение `UNMATCHED_RECEIPT`, не авто-обещание. Политика приоритетов (VIP) — policy-hook на будущее.

### D7. Retry/DLQ/timers *(дефолт; «не отменять заказ» — подтверждено)*
Temporal-retry: транспортные ошибки — exponential 1s→30s, 10 попыток; бизнес-отказы (`Shortage`, отклонение guard'а) — non-retriable, сразу в ветку саги. Legacy-адаптер: poison-события в таблицу DLQ в монолите (конвенции DLQ в Odoo нет) + алерт. Durable timers: `PromiseExpiring`, тайм-ауты шагов, эскалация buyer. Истечение TTL и shortage НЕ отменяют заказ — долгий `EXCEPTION_WAITING`.

### D8. Watchdog два уровня; SUSPENDED вместо компенсации *(семантика подтверждена; пороги — дефолт)*
Tier 1 (self-heal): дрейф `Σ promises` против ATP > ε1 (1% на SKU-партицию) → hold новых обещаний + пересчёт из replay. Tier 2 (harakiri): oversell сверх буфера или дрейф реплики > ε2 (1 ед.) → отказ write-команд, `exit(1)`, рестарт с full-reconcile до re-admit. Оркестратор при недоступном/fail-closed Inventory переводит саги в **SUSPENDED, не в compensating** (компенсация против полумёртвого состояния удваивает эффекты). *Якоря в коде:* `stock_quant._clean_reservations:1174` (лечащий сверщик → становится watchdog'ом), `stock_move._check_quantity:2346` (встроенный детектор → триггер Tier 2). Харакри заменяет роль, которую в монолите играли откат транзакции и `SKIP LOCKED`-fail-fast (`odoo/orm/models.py:5084`).

### D9. Приёмка = контракты + два сквозных сценария *(дефолт, но обязателен в tasks)*
Consumer-driven контракты на каждую пару участников; golden E2E: confirm→shortage→replenish→приход→promise→ship→сверка проекций; crash-тест: kill Inventory на шаге PromiseCreated → восстановление без задвоения. Ни один «дефолт» не считается принятым без этих двух сценариев.

### D10. picking_policy — 1:1 перенос семантики *(дефолт)*
'one' = wait-all (сага не уходит в planned, пока выделены не все строки), 'direct' = per-line sub-states. Это единственный старый UX-термин, переживающий сплит без изменения; берётся из текущих веток (`_select_expected_date`, `move_type` по `picking_policy`, `sale_stock/models/stock.py:274-283`).

### D11. Инфраструктурные фиксации *(подтверждено)*
Оркестратор — standalone `fulfillment-orchestrator` (Temporal, Python); шина — Kafka; outbox-публикация — отдельный relay-poller по outbox-таблице (не `cr.postcommit`-публикация: она at-most-once при падении между commit и publish, хук годится только как wake-up, `odoo/sql_db.py:259`); CDC — Debezium-поток из БД монолита. **Kafka-адаптеры монолита — внешние процессы** (consume + вызов ORM через штатные endpoint'ы Odoo), а не консьюмеры внутри workers: курсорный lifecycle Odoo несовместим с долгоживущим consumer-потоком.

## Risks / Trade-offs

- [Лаг CDC → promise по устаревшему ATP → oversell] → принятая бизнес-семантика (D1 buffer, SLO-lag hold из спеки `inventory-promises`); Tier-2 watchdog как последний рубеж.
- [`security_lead` ≠ «запас на исполнение» по смыслу] → параметризованный `ship_sla_margin` отдельно от `security_lead`; значение калибруется после shadow-фазы (см. Open Questions).
- [PO merge между заказами → компенсация ограничена `POLineDecrease`] → сага не планирует «cancel PO»; при отмене заказа после частичного прихода — возврат через exception-ветку (вне фазы-1).
- [Устаревшая проекция qty_delivered в guard'е] → финальная проверка в Inventory (D5); UI-ошибка может отличаться от серверной — принято как price of eventual.
- [Temporal — новый stateful-компонент] → минимальный кластер, isolated namespace; деградация саг без Temporal не предусмотрена (флаг откатывает на legacy-путь целиком).
- [Несколько независимых консьюмеров одного типа расходятся при рестарте] → монотонный `seq` + last-write-wins на проекциях (D5), идемпотентность команд (D3).
- [Права/company-scoping в новых сервисах] → security-QA гейт: CDC-контракты и фильтрация company в ATP/shortage реестрах (matrix в proposal).

## Migration Plan

Порядок включения (каждый шаг реверсиблен):

1. **Подготовка**: топики, Debezium slot, outbox-addon в монолите (только запись outbox — поведение не меняется).
2. **Shadow**: inventory-overlay стартует consumer-only, считает ATP и сверяет с текущим `free_qty`/`virtual_available` монолита; метрика дрейфа = критерий выхода из shadow. Пороги D8 калибруются по данным shadow.
3. **Orchestrator + адаптеры** развёрнуты, саги никуда не отправляются (dry-run log).
4. **Canary флаг (per-company)**: подтверждение с флагом → оптимистичный путь + PromiseStock; старый синхронный `_action_launch_stock_rule` остаётся в коде под веткой флага. Откат = выключение флага: физические данные не покидали монолит, PromiseLedger отбрасывается и строится заново replay-ем событий (rollback-требование extraction-rules §3 выполняется «по конструкции»).
5. **Expansion** по компаниям; старая синхронная ветка объявляется deprecated (удаление — вне фазы-1).

## Open Questions

- Выбор Kafka-дистрибутива и режима Debezium (Server vs embedded) — операционный, не влияет на спеки/задачи.
- Значения `ship_sla_margin` и ε2 по умолчанию — калибруются из shadow-метрик.
- Представление `allocation_state` в portal/чатах (UI-тексты) — после принятия контрактов событий.
- JSON→registry миграция — при первом внешнем потребителе.
