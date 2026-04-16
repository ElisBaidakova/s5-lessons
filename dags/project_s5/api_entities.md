Список полей, которые необходимы для витрины:

id — идентификатор записи.
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга.
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Список таблиц в слое DDS, из которых берутся поля для витрины:

dm_couriers (courier_id, courier_name)  -- НУЖНО СОЗДАТЬ
dm_timestamps (settlement_year = year, settlement_month = month)  -- есть в хранилище
dm_orders (orders_count = COUNT(order_key))  -- есть в хранилище
fct_product_sales (orders_total_sum = SUM(total_sum), order_processing_fee = SUM(total_sum) * 0.25)  -- есть в хранилище
dm_courier_reward (courier_tips_sum - возьмём значение из dds; rate_avg, courier_order_sum, courier_reward_sum - будет вычисляться в витрине)  -- НУЖНО СОЗДАТЬ

Список сущностей и полей, которые необходимо загрузить из API:
из restaurants: _id, name
из couriers: _id, name
из deliveries: order_id, order_ts, courier_id, rate, sum, tip_sum
