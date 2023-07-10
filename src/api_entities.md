Список полей, которые необходимы для витрины.
Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. Отметьте, какие таблицы уже есть в хранилище, а каких пока нет. Недостающие таблицы вы создадите позднее. Укажите, как они будут называться.

CREATE TABLE cdm.dm_courier_ledger (
	id serial NOT NULL,
	courier_id varchar(100) NOT NULL, --API COURIERS (dds.dm_couriers)
	courier_name varchar(100) NOT NULL, --API COURIERS (dds.dm_couriers)
	settlement_year integer NOT NULL, --dds.dm_orders
	settlement_month integer NOT NULL, --dds.dm_orders
	orders_count integer NOT NULL, --API DELIVERIES (dds.dm_deliveries) Берем из АПИ, т.к. нужны только доставки, а не все заказы
	orders_total_sum numeric(14, 2) NOT NULL, ---API DELIVERIES (dds.dm_deliveries) Берем из АПИ, т.к. нужны только доставки, а не все заказы
	rate_avg numeric(14, 2) NOT NULL, --API DELIVERIES (dds.dm_deliveries)
	order_processing_fee numeric(14, 2) NOT NULL, --API DELIVERIES (dds.dm_deliveries) Берем из АПИ, т.к. нужны только доставки, а не все заказы
	courier_order_sum numeric(14, 2) NOT NULL, --API DELIVERIES (dds.dm_deliveries) Берем из АПИ, т.к. нужны только доставки, а не все заказы
	courier_tips_sum numeric(14, 2) NOT NULL, --API DELIVERIES (dds.dm_deliveries)
	courier_reward_sum numeric(14, 2) NOT NULL, --API DELIVERIES (dds.dm_deliveries)
	
	
	CONSTRAINT dm_courier_ledger_report_settlement_year_check CHECK (settlement_year > 0),
	CONSTRAINT dm_courier_ledger_report_settlement_month_check CHECK (settlement_month > 0),
	CONSTRAINT dm_courier_ledger_report_orders_count_check CHECK (orders_count >= 0),
	CONSTRAINT dm_courier_ledger_report_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric),
	CONSTRAINT dm_courier_ledger_report_rate_avg_check CHECK (rate_avg >= (0)::numeric),
	CONSTRAINT dm_courier_ledger_report_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric),
	CONSTRAINT dm_courier_ledger_report_order_courier_order_sum_check CHECK (courier_order_sum >= (0)::numeric),
	CONSTRAINT dm_courier_ledger_report_order_courier_tips_sum_check CHECK (courier_tips_sum >= (0)::numeric),
	CONSTRAINT dm_courier_ledger_report_order_courier_reward_sum_check CHECK (courier_reward_sumы >= (0)::numeric),

	
	CONSTRAINT pk_dm_courier_ledger_report PRIMARY KEY (id),
	CONSTRAINT unique_dm_courier_ledger_report UNIQUE (courier_id, settlement_year, settlement_month)
);        

На основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API. Использовать все методы API необязательно: важно загрузить ту информацию, которая нужна для выполнения задачи.

GET /couriers:
_id — ID курьера в БД;
name — имя курьера.

GET / deliveries:
order_id — ID заказа;
courier_id — ID курьера;
delivery_ts — дата и время совершения доставки;
rate — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;
sum — сумма заказа (в руб.);
tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).