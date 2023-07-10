import os
from logging import Logger
from pathlib import Path

from lib import PgConnect


class CDMLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def load_cdm(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                         """
                         DELETE FROM cdm.dm_settlement_report;

                        insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                        select  r.id as restaurant_id, 
                                r.restaurant_name, 
                                d.date as settlement_date, 
                                count(distinct s.order_id) as orders_count, 
                                sum(s.total_sum) as orders_total_sum, 
                                sum(s.bonus_payment) as orders_bonus_payment_sum, 
                                sum(s.bonus_grant) as orders_bonus_granted_sum, 
                                sum(s.total_sum * 0.25) as order_processing_fee, 
                                sum(s.total_sum - s.total_sum * 0.25 - s.bonus_payment) as restaurant_reward_sum
                        from dds.dm_restaurants r 
                            inner join dds.dm_orders o on r.id = o.restaurant_id 
                            inner join dds.fct_product_sales s on s.order_id = o.id 
                            inner join dds.dm_timestamps d on d.id = o.timestamp_id 
                        where r.active_to > now() and o.order_status = 'CLOSED'
                        group by  r.id, r.restaurant_name, d.date
                        ON CONFLICT (restaurant_id, settlement_date) 
                        DO UPDATE 
                        SET
                            orders_count = EXCLUDED.orders_count,
                            orders_total_sum = EXCLUDED.orders_total_sum,
                            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                            order_processing_fee = EXCLUDED.order_processing_fee,
                            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;

                         """
                    )

                    cur.execute(
                         """
                         DELETE FROM cdm.dm_courier_ledger;

                        WITH base AS (
                        SELECT
                            d.courier_id,
                            dt."year" AS order_year,
                            dt."month" AS order_month,
                            AVG(d.rate) AS avg_rate
                        FROM
                            dds.dm_deliveries d 
                            LEFT JOIN (SELECT DISTINCT id, delivery_id, timestamp_id FROM dds.dm_orders) o ON d.id = o.delivery_id
                            INNER JOIN dds.dm_timestamps dt ON o.timestamp_id = dt.id 
                        GROUP BY
                            d.courier_id,
                            dt."year",
                            dt."month"
                        ),
                                                
                                                
                                                

                        BASE2 as (
                            SELECT
                        dc.courier_id AS courier_id,
                        dc.courier_name AS courier_name,
                        EXTRACT(YEAR FROM delivery_ts) AS settlement_year,
                        EXTRACT(MONTH FROM delivery_ts) AS settlement_month,
                        COUNT(DISTINCT d.order_id) AS orders_count,
                        SUM(d.sum) AS orders_total_sum,
                        b.avg_rate AS rate_avg,
                        SUM(d.sum * 0.25) AS order_processing_fee,
                        SUM(d.tip_sum) AS courier_tips_sum,
                        SUM( CASE
                        WHEN d.rate < 4 or d.rate is NULL THEN
                            CASE
                            WHEN d.sum * 0.05 < 100 THEN 100
                            ELSE d.sum * 0.05
                            END
                        WHEN d.rate < 4.5 THEN
                            CASE
                            WHEN d.sum * 0.07 < 150 THEN 150
                            ELSE d.sum * 0.07
                            END
                        WHEN d.rate < 4.9 THEN
                            CASE
                            WHEN d.sum * 0.08 < 175 THEN 175
                            ELSE d.sum * 0.08
                            END
                        
                        ELSE
                            CASE
                            WHEN d.sum * 0.1 < 200 THEN 200
                            ELSE d.sum * 0.1
                            END
                        END) AS courier_order_sum 
                        FROM
                        dds.dm_deliveries d 
                        INNER JOIN dds.dm_couriers dc ON dc.id = d.courier_id
                        LEFT JOIN base b ON b.courier_id = d.courier_id AND b.order_year = EXTRACT(YEAR FROM delivery_ts) AND b.order_month = EXTRACT(MONTH FROM delivery_ts) 
                        GROUP BY
                        dc.courier_id,
                        dc.courier_name,
                        EXTRACT(YEAR FROM delivery_ts),
                        EXTRACT(MONTH FROM delivery_ts),
                        b.avg_rate
                                                
                        )
                        insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                        select courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, coalesce(rate_avg, 0), order_processing_fee, courier_order_sum, courier_tips_sum, courier_order_sum * 0.95 AS courier_reward_sum
                        FROM BASE2

                         """
                    )
