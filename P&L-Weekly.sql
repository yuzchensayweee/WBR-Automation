SET TIME ZONE 'America/Los_Angeles';
-- Get Weekly Gross Rev & Net Rev for the past 10 Weeks
WITH region_base AS (
SELECT delivery_week
, CASE WHEN sales_region_id = 24 OR lower(biz_type) LIKE '%pantry%' THEN 3 ELSE sales_region_id END AS sales_region_id_new
, CASE WHEN sales_region_id = 24 OR lower(biz_type) LIKE '%pantry%' THEN 'MAIL ORDER' ELSE sales_region_title END AS sales_region_title_new
, order_id
, buyer_id
, group_invoice_id
, MAX(daily_new_users_flag) AS daily_new_users_flag
, SUM(sub_total) AS sub_total
, SUM(CASE WHEN is_gm = 0 THEN sub_total ELSE 0 END) AS oos
, SUM(CASE WHEN is_gm = 1 THEN product_refund_amount ELSE 0 END) AS refund
, MAX(discount) AS coupon
, MAX(CASE WHEN coupon IN ('sign_up_coupon','second_order_coupon') THEN discount ELSE 0 END) AS new_user_coupon
, SUM(gm_cost*is_gm) AS cogs
, SUM(gm_margin) AS gm_margin
, SUM(gm_base) AS gm_base
, dense_rank() OVER (PARTITION BY date(date_trunc('month', delivery_week)) ORDER BY delivery_week DESC) AS rk
,'' as place_holder
FROM metrics.order_product
WHERE delivery_week BETWEEN date(dateadd('week', -9,dateadd(d, - datepart(dow, current_date), current_date)::date)) AND date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
AND order_biz_type IN ('grocery','alcohol')
GROUP BY 1,2,3,4,5,6
)
-- Sales Base Data for regional P&L
, region_rev as (
    SELECT  delivery_week
        , rk
        , sales_region_id_new AS sales_region_id
        , sales_region_title_new AS sales_region_title
        , 0.023 AS weeebates
        , SUM(daily_new_users_flag) AS new_user
        , COUNT(distinct buyer_id) AS active_user
        , SUM(sub_total) AS gross_rev
        , gross_rev/active_user AS arpu
        , SUM(coupon) AS total_coupon_rev
        , SUM(new_user_coupon) AS new_user_coupon
        -- , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , total_coupon_rev - 10*new_user as coupon_rev
        , CASE WHEN delivery_week = date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))
               AND date_diff('day', delivery_week, current_date) = 8 THEN 
               CASE WHEN sales_region_id_new = 3 THEN sum(refund)/0.5 else sum(refund)/0.9 END
          ELSE SUM(refund) END AS refund_rev
        , SUM(oos) as oos_rev
        , oos_rev + refund_rev as total_refund_rev
        , SUM(cogs) as cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 as net_rev
    from region_base
    group by 1,2,3,4,5
)
-- Sales Base Data for Company P&L
, groc_rev AS (
    select delivery_week
        , rk
        , 999 AS sales_region_id
        , 'Company Total' AS sales_region_title
        , 0.023 as weeebates
        , SUM(daily_new_users_flag) AS new_user
        , count(distinct buyer_id) AS active_user
        , SUM(sub_total) AS gross_rev
        , gross_rev/active_user AS arpu
        , SUM(coupon) AS total_coupon_rev
        , SUM(new_user_coupon) AS new_user_coupon
        -- , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , total_coupon_rev - 10*new_user AS coupon_rev
        , SUM(refund) AS refund_rev
        , SUM(oos) AS oos_rev
        , oos_rev + refund_rev AS total_refund_rev
        , SUM(cogs) AS cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 AS net_rev
    FROM region_base
    GROUP BY 1,2,3,4,5
)
-- Reported P&L
,  reported_pl_base AS (
    select * from
    (select * from sandbox.ggs_ba_pl union all select * from sandbox.ggs_la_pl union all
    select * from sandbox.ggs_nj_pl union all select * from sandbox.ggs_sea_pl union all
    select * from sandbox.ggs_fl_pl union all select * from sandbox.ggs_tx_pl union all
    select * from sandbox.ggs_mo_pl union all select * from sandbox.ggs_groc_pl union all
    select * from sandbox.ggs_chi_pl) t
    where delivery_month like '%\'%'
)
, reported_pl as (
    select region_id
        , sales_region_title
        , to_date(delivery_month, 'Mon YY') as report_month
        , dense_rank()over(partition by region_id order by to_date(delivery_month, 'Mon YY') desc) as rn
        , inbound_pct::float as inbound_pct
        , wh_labor_pct::float as wh_labor
        , packaging_pct::float as packaging_pct
        , processing_pct::float as processing_fee_pct
        , 0.001 as css
        , wh_pct::float as wh_pct
        , ops_pct::float as ops_pct
        , misc_pct::float as misc_pct
    from reported_pl_base
)

-- Inbound feight: past 4 months avg. pct

-- Warehouse Fulfillment Cost (Labor): 
-- Packaging Material: past 4 months avg. pct 
-- Processing Fee: last month pct
-- Customer Service: 0.001 

-- Warehouse Fulfillment Cost (Non-labor):
-- Warehouse: last month weekly dollar value 
-- Ops Mgmt: last month weekly dollar value 
-- Misc: last month weekly dolla value 

, region_pl as (
  select pl.report_month
  , pl.region_id
  , avg(pl4.inbound_pct) as avg_inbound_pct
  , max(pl.inbound_pct) as inbound_pct
  , max(pl.packaging_pct) as packaging_pct
  , avg(pl3.packaging_pct) as avg_packaging_pct
  , max(pl.processing_fee_pct) as processing_fee_pct
  , max(pl.wh_pct) as wh_pct
  , avg(pl.wh_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_wh_dollar_value
  , max(pl.ops_pct) as ops_pct
  , avg(pl.ops_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_ops_dollar_value
  , max(pl.misc_pct) as misc_pct
  , avg(pl.misc_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_misc_dollar_value
from reported_pl pl
left join (select * from region_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))))
           ) rr on pl.region_id = rr.sales_region_id -- Last month weekly net revenue (regional)
left join  (select * from groc_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))))
            ) gr on pl.region_id  = gr.sales_region_id -- Last month weekly net revenue (company total)
left join (select report_month, region_id, packaging_pct from reported_pl where rn <= 4 ) pl3 ON pl.region_id = pl3.region_id
left join (select report_month, region_id, inbound_pct from reported_pl where rn <= 4 ) pl4 on pl.region_id = pl4.region_id
where rn = 1
group by 1,2
)
, sales_base_wkly_region as (
    select  rr.*
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_inbound_pct*rr.net_rev else inbound_pct*rr.net_rev end as inbound_rev
        , processing_fee_pct*rr.net_rev as processing_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_packaging_pct*rr.net_rev else packaging_pct*rr.net_rev end as packaging_rev
        , 0.001 * rr.net_rev as cs_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_wh_dollar_value else wh_pct*net_rev end as warehouse_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_misc_dollar_value else misc_pct*net_rev end as misc_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_ops_dollar_value else ops_pct*net_rev end as ops_rev
        , packaging_rev/net_rev as packaging_pct
        , processing_rev/net_rev as processing_pct
        , '' as place_holder
    from region_rev rr
    left join (select * from region_pl where region_id != 999) pl on rr.sales_region_id = pl.region_id
    where delivery_week between date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
)
,  sales_base_wkly_groc as (
    select  gr.*
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_inbound_pct*net_rev else inbound_pct*net_rev end as inbound_rev
        , processing_fee_pct*net_rev as processing_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_packaging_pct*gr.net_rev else packaging_pct*gr.net_rev end as packaging_rev
        , 0.001*net_rev as cs_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_wh_dollar_value else wh_pct*net_rev end as warehouse_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_misc_dollar_value else misc_pct*net_rev end as misc_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then avg_ops_dollar_value else ops_pct*net_rev end as ops_rev
        , packaging_rev/net_rev as packaging_pct
        , processing_rev/net_rev as processing_pct
        , '' as place_holder
    from groc_rev gr
    left join (select * from region_pl where region_id = 999) pl on gr.sales_region_id = pl.region_id
    where delivery_week between date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
)
, delivery_base_wkly_region_nomo as (
select  delivery_week,
        sales_region_id,
        sum(total_delivery_cost)+sum(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   delivery_week between date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_wkly_region_mo as (
select  delivery_week,
        3 as sales_region_id,
        sum(total_delivery_cost) as delivery_cost
        from (
            select dateadd('day', -date_part(dow, deal_delivery_date::date)::int, deal_delivery_date::date)::date as delivery_week
            , wms_order_id
            , max(rate) as total_delivery_cost
            from metrics.mail_order_shipment
            where delivery_week between date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) AND date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
            group by 1,2
        ) mo
group by 1,2
)
, delivery_base_wkly_region as (
select * from delivery_base_wkly_region_nomo union all select * from delivery_base_wkly_region_mo
)
, delivery_base_wkly_groc as ( select delivery_week, 999 as sales_region_id, sum(delivery_cost) as delivery_cost from delivery_base_wkly_region group by 1,2)

-- Separate Line for Labor Cost
, p_and_l_labor AS (
SELECT delivery_week::date
, sales_region_id
, inbound_pct::float
FROM (SELECT * FROM sandbox.ggs_nj_labor  WHERE delivery_week::date < DATEADD('week', -1, dateadd(d, - datepart(dow, current_date), current_date)::date)
UNION ALL
SELECT * FROM sandbox.ggs_chi_labor WHERE delivery_week::date < DATEADD('week', -1, dateadd(d, - datepart(dow, current_date), current_date)::date) ) t
ORDER BY 2,1
)
, adp_tbl AS (-- Exclude LA - Greater
SELECT CASE WHEN entry_week = '2022-10-09' THEN entry_date ELSE entry_week END AS entry_week
        , CASE WHEN  warehouse = 'FL - Tampa' THEN 23
            WHEN warehouse = 'TX - Houston' THEN 10
            WHEN warehouse = 'WA - Seattle' THEN 4
            WHEN warehouse = 'IL - Chicago' THEN 15
            WHEN warehouse = 'LA - La Mirada' THEN 2
            WHEN warehouse = 'NJ - Edison' THEN 7
            WHEN warehouse = 'SF - Union City' THEN 1 END AS sales_region_id
        , SUM(labor_cost) AS total_cost
        , SUM(CASE WHEN sales_region_id = 2 AND department_name NOT LIKE '%OP - DC%' THEN labor_cost ELSE 0 END) AS total_cost_ex_dc
        , SUM(CASE WHEN sales_region_id = 2 AND lower(department_name) LIKE '%dry%' THEN labor_cost ELSE 0 END) AS dry_cost
        , SUM(CASE WHEN sales_region_id in (15,7) AND lower(department_name) LIKE '%mail order - outbound%' THEN labor_cost ELSE 0 END ) AS mo_outbound
FROM metrics.adp_labor_cost_detail
WHERE entry_week BETWEEN date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
AND warehouse != 'check' AND warehouse != 'LA - Greater'
and filter_flag = 'N'
group by 1,2
)
-- La Mirada Revenue %
,la_base  AS (
    SELECT CASE WHEN delivery_week = '2022-10-09' THEN delivery_day ELSE delivery_week END AS delivery_week
    , CASE WHEN sales_region_id in (1,2) THEN sales_region_id ELSE 3 END AS sales_region_id
    , SUM(CASE WHEN storage_type = 'N' THEN sub_total ELSE 0 END) as dry_rev
    , SUM(sub_total) AS gross_rev
    FROM metrics.order_product
    WHERE delivery_week  BETWEEN date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
    AND inventory_title = 'LA - La Mirada'
    AND order_biz_type IN ('grocery','alcohol')
    AND payment_mode = 'F'
    GROUP BY 1,2
)
, la AS (SELECT delivery_week , SUM(dry_rev) as total_dry_rev, SUM(gross_rev) as total_gross_rev FROM la_base GROUP BY 1)
, la_pct as (
    SELECT labr.delivery_week
        , labr.sales_region_id
        , CASE WHEN labr.sales_region_id = 1 AND labr.delivery_week <= '2022-10-14' THEN labr.dry_rev / lab.total_dry_rev
               WHEN labr.sales_region_id = 1 AND labr.delivery_week >= '2022-10-15' THEN labr.gross_rev/lab.total_gross_rev
               WHEN labr.sales_region_id = 3 THEN labr.dry_rev/lab.total_dry_rev ELSE 0 END AS rev_pct
    FROM la_base labr
    LEFT JOIN la  lab on labr.delivery_week = lab.delivery_week
)
-- Chicago & New York Revenue %

, cn_base AS (
    SELECT  CASE WHEN delivery_week = '2022-10-09' THEN delivery_day ELSE delivery_week END AS delivery_week
        , CASE WHEN inventory_title = 'IL - Chicago' THEN 15 ELSE 7 END AS sales_region_id
        , SUM(CASE WHEN biz_type like '%pantry%' OR sales_region_id = 3 OR LOWER(sales_region_title) like '%mof%' THEN sub_total ELSE 0 END) AS mo_rev
        , SUM(sub_total) as total_rev
    FROM metrics.order_product
    WHERE delivery_week BETWEEN date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
    AND order_biz_type IN ('grocery','alcohol')
    AND inventory_title IN ('IL - Chicago','NJ - New York')
    GROUP BY 1,2
)
, cn  as (SELECT delivery_week, sales_region_id, mo_rev/total_rev AS mo_pct FROM cn_base)
, labor_base AS (
    SELECT entry_week
        , adp.sales_region_id
        , adp.total_cost
        , adp.total_cost_ex_dc
        , dry_cost
        , mo_outbound
        , CASE WHEN adp.sales_region_id in (15,7) then total_cost*pl.inbound_pct*cn.mo_pct
               WHEN adp.sales_region_id = 2 THEN dry_cost*la.rev_pct ELSE 0 END AS mo_labor
        , CASE WHEN adp.sales_region_id = 2 AND adp.entry_week <= '2022-10-14' THEN adp.dry_cost * la1.rev_pct
               ELSE adp.total_cost_ex_dc*la1.rev_pct END AS allo_cost  --> LA Mirada to Bay Area
    FROM adp_tbl  adp
    LEFT JOIN la_pct la on adp.entry_week = la.delivery_week and la.sales_region_id = 3
    LEFT JOIN la_pct la1 on adp.entry_week = la1.delivery_week and la1.sales_region_id = 1
    LEFT JOIN cn on adp.entry_week = cn.delivery_week and adp.sales_region_id = cn.sales_region_id
    LEFT JOIN p_and_l_labor pl on DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE = date(pl.delivery_week) and adp.sales_region_id = pl.sales_region_id
)
, labor_cost_nomo as (
    SELECT t1.entry_week
        , t1.sales_region_id
        , t1.total_cost
        , nvl(lagt.labor_cost,0) as la_greater_labor_cost
        , CASE WHEN t1.sales_region_id = 1 THEN t2.allo_cost ELSE t1.allo_cost END AS allo_cost
        , t1.mo_outbound
        , t1.mo_labor
        , CASE WHEN t1.sales_region_id NOT IN (2,1,15,7) THEN t1.total_cost
               WHEN t1.sales_region_id = 2 -- LA (Total cost - BA allocation - MO allocation + LA Greater labor cost)
               THEN t1.total_cost - t1.mo_labor - t1.allo_cost + la_greater_labor_cost
               WHEN t1.sales_region_id = 1 -- BA (Total labor cost + BA allocation)
               THEN t1.total_cost + t2.allo_cost
               WHEN t1.sales_region_id in (15,7) THEN t1.total_cost - t1.mo_outbound - t1.mo_labor
           END AS final_labor_cost
    FROM labor_base t1
    LEFT JOIN labor_base t2 on t1.entry_week = t2.entry_week and t2.sales_region_id = 2
    LEFT JOIN (SELECT CASE WHEN entry_week = '2022-10-09' THEN entry_date ELSE entry_week END AS entry_week, SUM(labor_cost) AS labor_cost FROM metrics.adp_labor_cost_detail WHERE filter_flag = 'N' and warehouse = 'LA - Greater' GROUP BY 1) lagt
       ON t1.sales_region_id = 2 AND t1.entry_week = lagt.entry_week )
, labor_cost_mo AS (
    SELECT entry_week
          , 3 as sales_region_id
          , SUM(mo_outbound)+SUM(mo_labor) AS total_cost
          , 0 AS la_greater_labor_cost
          , 0 AS allo_cost
          , SUM(mo_outbound) as mo_outbound
          , SUM(mo_labor) as mo_labor
          , SUM(mo_outbound)+sum(mo_labor) as final_labor_cost
    from labor_cost_nomo
    GROUP BY 1,2,4,5)
, adp_cost_region AS ( 
SELECT CASE WHEN entry_week BETWEEN '2022-10-09' AND '2022-10-15' THEN DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE ELSE entry_week END AS entry_week
, sales_region_id
, SUM(total_cost) as total_cost
, SUM(la_greater_labor_cost) as la_greater_labor_cost
, SUM(allo_cost) as allo_cost
, SUM(mo_outbound) as mo_outbound
, SUM(mo_labor) as mo_labor
, SUM(final_labor_cost) as final_labor_cost
FROM labor_cost_nomo
GROUP BY 1,2
UNION ALL
SELECT CASE WHEN entry_week BETWEEN '2022-10-09' AND '2022-10-15' THEN DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE ELSE entry_week END AS entry_week
, sales_region_id
, SUM(total_cost) as total_cost
, SUM(la_greater_labor_cost) as la_greater_labor_cost
, SUM(allo_cost) as allo_cost
, SUM(mo_outbound) as mo_outbound
, SUM(mo_labor) as mo_labor
, SUM(final_labor_cost) as final_labor_cost
FROM labor_cost_mo
GROUP BY 1,2
    )
, adp_cost_groc AS (
select  entry_week
    , 999 as sales_region_id
    , SUM(total_cost) as total_cost
    , SUM(la_greater_labor_cost) as la_greater_labor_cost
    , SUM(allo_cost) as allo_cost
    , SUM(mo_outbound) as mo_outbound
    , SUM(mo_labor) as mo_labor
    , SUM(final_labor_cost) as final_labor_cost
    FROM adp_cost_region
    GROUP BY 1,2
)
, pl_region AS (
    SELECT sb.*
    , delivery_cost as delivery_cost
    , delivery_cost*1.01 / net_rev as delivery_cost_pct
    , adp.total_cost
    , adp.la_greater_labor_cost
    , adp.allo_cost
    , adp.mo_outbound
    , adp.mo_labor
    , adp.final_labor_cost
    , adp.final_labor_cost/net_rev as wh_labor_pct
    FROM sales_base_wkly_region sb
    JOIN delivery_base_wkly_region db on sb.delivery_week = db.delivery_week and sb.sales_region_id = db.sales_region_id
    LEFT JOIN adp_cost_region adp on adp.sales_region_id = sb.sales_region_id and sb.delivery_week = adp.entry_week 
)
, pl_groc as (
    SELECT sb.*
    , delivery_cost as delivery_cost
    , delivery_cost*1.01 / net_rev as delivery_cost_pct
    , adp.total_cost
    , adp.la_greater_labor_cost
    , adp.allo_cost
    , adp.mo_outbound
    , adp.mo_labor
    , adp.final_labor_cost
    , adp.final_labor_cost/net_rev as wh_labor_pct
    FROM sales_base_wkly_groc sb
    JOIN delivery_base_wkly_groc db on sb.delivery_week = db.delivery_week and sb.sales_region_id = db.sales_region_id
    LEFT JOIN adp_cost_groc adp on adp.sales_region_id = sb.sales_region_id and sb.delivery_week = adp.entry_week
)
, pl_base as
(select * from pl_region union all select * from pl_groc order by 3,1)
select delivery_week
    , sales_region_id
    , sales_region_title
    , weeebates
    , new_user
    , active_user
    , gross_rev
    , total_coupon_rev
    , new_user_coupon
    , coupon_rev
    , arpu
    , refund_rev
    , oos_rev
    , total_refund_rev
    , cogs_rev
    , net_rev
    , inbound_rev
    , processing_rev
    , packaging_rev
    , warehouse_rev
    , misc_rev
    , cs_rev
    , ops_rev
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(total_cost) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE total_cost END AS total_cost
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(la_greater_labor_cost) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE la_greater_labor_cost END AS la_greater_labor_cost
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(allo_cost) OVER (PARTITION BY sales_region_id ORDER BY delivery_week) ELSE allo_cost END AS allo_cost
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(mo_outbound) OVER (PARTITION BY sales_region_id ORDER BY delivery_week) ELSE mo_outbound END AS mo_outbound
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(mo_labor) OVER (PARTITION BY sales_region_id ORDER BY delivery_week) ELSE allo_cost END AS mo_labor
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(final_labor_cost) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE delivery_cost END AS final_labor_cost
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(delivery_cost) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE delivery_cost END AS delivery_cost
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(delivery_cost_pct) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE delivery_cost_pct END AS delivery_cost_pct
    , packaging_pct
    , processing_pct
    , CASE WHEN delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date THEN LAG(wh_labor_pct) OVER (PARTITION BY sales_region_id  ORDER BY delivery_week) ELSE wh_labor_pct END AS wh_labor_pct
from pl_base
order by 2,1;