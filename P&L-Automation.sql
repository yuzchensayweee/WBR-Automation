-- calculate weekly net rev base
with region_base as (
    select delivery_day
        , case when sales_region_id = 24  or lower(biz_type) like '%pantry%' then 3 --> mof to mo
               else sales_region_id end as sales_region_id_new
        , case when sales_region_id = 24 or lower(biz_type) like '%pantry%' then 'MAIL ORDER' else sales_region_title end as sales_region_title_new
        , order_id
        , buyer_id
        , group_invoice_id
        , max(daily_new_users_flag) as daily_new_users_flag
        , sum(sub_total) as sub_total
        , sum(case when is_gm = 0 then sub_total else 0 end) as oos
        , sum(case when is_gm = 1 then product_refund_amount else 0 end) as refund
        , max(discount) as coupon
        , max(case when coupon in ('sign_up_coupon','second_order_coupon') then discount else 0 end) as new_user_coupon
        , sum(gm_cost*is_gm) as cogs
        , sum(gm_margin) as gm_margin
        , sum(gm_base) as gm_base
        , dense_rank()
        over
        (partition by date_trunc('month', dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date) order by dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date desc) as rk
        ,'' as place_holder
    from metrics.order_product
    where (date_trunc('month',delivery_week)::date = dateadd('month', -1, date_trunc('month', current_date))::date
    or delivery_week between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1)))
    and order_biz_type in ('grocery','alcohol')
    group by 1,2,3,4,5,6
)
-- avg net rev
, region_rev as (
    select  dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date as delivery_week
        , rk
        , sales_region_id_new as sales_region_id
        , sum(daily_new_users_flag) as new_user
        , sum(sub_total) as gross_rev
        , sum(refund) as refund
        , sum(oos)+sum(refund) as oos_rev
        , sum(coupon) as coupon_rev
        , sum(coupon) - sum(new_user_coupon) as discount
        , gross_rev - oos_rev - (coupon_rev-10*new_user) - gross_rev*0.023 as net_rev
        , sum(refund) as return_rev
    from region_base
    group by 1,2,3
)
-- calculate weekly company net rev
, groc_base as (
    select delivery_day
        , 999 as sales_region_id
        , order_id
        , buyer_id
        , group_invoice_id
        , max(daily_new_users_flag) as daily_new_users_flag
        , sum(sub_total) as sub_total
        , sum(case when is_gm = 0 then sub_total else 0 end) as oos
        , sum(case when is_gm = 1 then product_refund_amount else 0 end) as refund
        , max(discount) as coupon
        , max(case when coupon in ('sign_up_coupon','second_order_coupon') then discount else 0 end) as new_user_coupon
        , sum(gm_cost*is_gm) as cogs
        , sum(gm_margin) as gm_margin
        , sum(gm_base) as gm_base
        , dense_rank()
        over
        (partition by date_trunc('month', dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date) order by dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date desc) as rk
    from metrics.order_product
    where (date_trunc('month',delivery_week)::date = dateadd('month', -1, date_trunc('month', current_date))::date
    or delivery_week between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1)))
    and order_biz_type in ('grocery','alcohol')
    group by 1,2,3,4,5
)
, groc_rev as (
    select  dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date as delivery_week
        , sales_region_id
        , rk
        , sum(daily_new_users_flag) as new_user
        , sum(sub_total) as gross_rev
        , sum(oos)+sum(refund) as oos_rev
        , sum(coupon) as coupon_rev
        , sum(coupon) - sum(new_user_coupon) as discount
        , gross_rev - oos_rev - (coupon_rev-10*new_user) - gross_rev*0.023 as net_rev
        , sum(refund) as return_rev
    from groc_base
    group by 1,2,3
)
,  region_pl_base as (
    SELECT * FROM sandbox.ggs_ba_pl UNION ALL SELECT * FROM sandbox.ggs_la_pl UNION ALL
    SELECT * FROM sandbox.ggs_sea_pl UNION ALL SELECT * FROM sandbox.ggs_nj_pl UNION ALL
    SELECT * FROM sandbox.ggs_tx_pl UNION ALL SELECT * FROM sandbox.ggs_chi_pl UNION ALL
    SELECT * FROM sandbox.ggs_fl_pl UNION ALL SELECT * FROM sandbox.ggs_groc_pl UNION ALL
    SELECT * FROM sandbox.ggs_mo_pl
)
, region_pl_tbl as
(SELECT to_date(original_month,'Mon YY') as report_month
      , dense_rank()over(order by report_month desc) as rn
      , region_id
      , inbound::float AS inbound
      , wh_labor::float as wh_labor
      , packaging::float as packaging
      , payment::float as payment
      , css::float as css
      , wh_facility::float as wh_facility
      , ops_mgmt::float as ops
      , misc::float as misc
      , weee_points::float weee_points
FROM region_pl_base)
-- ops, misc, wh_facility (planned use avg. dollar value)
, region_pl as (
  SELECT pl.report_month
  , pl.region_id
  , pl.weee_points/coalesce(gross_rev_region, gross_rev_company) AS weeebates_pct
  , max(pl.inbound) as inbound_pct
  , max(pl.wh_labor) as wh_labor_pct
  , max(pl.packaging) as packaging_pct
  , avg(pl.packaging * coalesce(gr.net_rev, rr.net_rev)) as avg_packaging_dollar_value
  , avg(pl3.packaging) as packaging_pct_avg
  , max(pl.payment) as processing_fee_pct
  , max(pl.css) as cs_pct
  , avg(pl.css * coalesce(gr.net_rev, rr.net_rev)) as avg_cs_dollar_value
  , max(pl.wh_facility) as wh_pct
  , avg(pl.wh_facility * coalesce(gr.net_rev, rr.net_rev)) as avg_wh_dollar_value
  , max(pl.ops) as ops_pct
  , avg(pl.ops * coalesce(gr.net_rev, rr.net_rev)) as avg_ops_dollar_value
  , max(pl.misc) as misc_pct
  , avg(pl2.misc) as misc_pct_avg
--   , avg(pl.misc * coalesce(gr.net_rev, rr.net_rev)) as avg_misc_dollar_value
from region_pl_tbl pl
left join (select delivery_month, sales_region_id, sum(sub_total) as gross_rev_region from metrics.order_product where order_biz_type = 'grocery' group by 1,2) op
  on pl.report_month = op.delivery_month and pl.region_id = sales_region_id
left join (select delivery_month, 999 as sales_region_id, sum(sub_total) as gross_rev_company from metrics.order_product where order_biz_type  = 'grocery' group by 1,2) op1
  on pl.report_month = op1.delivery_month and pl.report_month = op1.delivery_month
left join (select * from region_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', current_date)) and rk <= 4 ) rr on pl.region_id = rr.sales_region_id
left join (select * from groc_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', current_date))and rk <= 4) gr on pl.region_id  = gr.sales_region_id
left join (select report_month, region_id, misc from region_pl_tbl where rn <= 2 ) pl2 ON pl.region_id = pl2.region_id
left join (select report_month, region_id, packaging from region_pl_tbl where rn <= 3 ) pl3 ON pl.region_id = pl3.region_id
where rn = 1
group by 1,2,3
)

/*weekly logic*/
-- weekly sales base
, sales_base_wkly_region as (
SELECT
  dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date::varchar as type,
  sales_region_id_new as sales_region_id,
  sales_region_title_new as sales_region_title,
  0.023 AS weeebates_pct,
  sum(daily_new_users_flag) as new_user,
  count(distinct buyer_id) as active_user,
  sum(sub_total) as gross_rev,
  count(distinct group_invoice_id) as delivery_cnt,
  round(gross_rev/active_user, 2) as arpu,
  round(gross_rev/delivery_cnt, 2) as rev_per_delivery,
  sum(coupon) as total_coupon_rev,
--   sum(new_user_coupon) as new_user_coupon_rev,
  -- total_coupon_rev - new_user_coupon_rev as coupon_rev,
  total_coupon_rev - 10*new_user as coupon_rev,
  coupon_rev/gross_rev as coupon_pct,
  -- max(weeebates_pct) as weee_bates_pct,
  -- coupon_pct+max(weeebates_pct) as discount_pct,
  coupon_pct+0.023 AS discount_pct,
  sum(oos) as oos_rev,
  oos_rev/gross_rev as oos_pct,
  case when type::date = dateadd('week', -1,date_trunc('week',current_date)-1) then sum(refund)/0.88 else sum(refund) end as refund_rev,
  refund_rev/gross_rev as refund_pct,
  oos_pct+refund_pct as oos_refund_pct,
  -- gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * max(weeebates_pct) as net_rev,
  gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * 0.023 as net_rev,
  net_rev/gross_rev as net_rev_pct,
  sum(cogs) as cogs_rev,
  sum(cogs)/gross_rev as cogs_pct,
  max(inbound_pct) as inbound_pct,
  (net_rev_pct-cogs_pct-max(inbound_pct)*net_rev_pct)/net_rev_pct as gm_pct,
  case when max(date_trunc('month', type::date)) = date_trunc('month', current_date) then max(packaging_pct_avg) else max(packaging_pct) end as packaging_pct,
  max(processing_fee_pct) as processing_fee_pct,
  max(cs_pct) as cs_pct,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_wh_dollar_value) else max(wh_pct) end as wh_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_wh_dollar_value)/net_rev
      else max(wh_pct) end as wh_pct,
--   case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_misc_dollar_value) else max(misc_pct) end as misc_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(misc_pct_avg)
      else max(misc_pct) end as misc_pct,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_ops_dollar_value) else max(ops_pct) end as ops_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_ops_dollar_value)/net_rev
      else max(ops_pct) end as ops_pct,
  -- max(wh_pct) as wh_pct,
  -- max(ops_pct) as ops_pct,
  -- max(misc_pct) as misc_pct,
  '' as placeholder
from region_base pl
left join (select * from region_pl where region_id != 999) rpl on pl.sales_region_id_new = rpl.region_id
where dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1))
group by 1,2,3,4
)
, delivery_base_wkly_region_nomo as (
select  delivery_week::varchar AS type,
        sales_region_id,
        sum(total_delivery_cost)+sum(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   delivery_week between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_wkly_region_mo as (
select  delivery_week::varchar AS type,
        3 as sales_region_id,
        sum(total_delivery_cost) as delivery_cost
        from (
            select dateadd('day', -date_part(dow, deal_delivery_date::date)::int, deal_delivery_date::date)::date as delivery_week
            , wms_order_id
            , max(rate) as total_delivery_cost
            from metrics.mail_order_shipment
            where delivery_week
            between date(dateadd('week', -4,date(date_trunc('week', current_date)-1)))
            and date(dateadd('week', -1,date_trunc('week', current_date)-1))
            group by 1,2
        ) mo
group by 1,2
)
, delivery_base_wkly_region as (
    select * from delivery_base_wkly_region_nomo union all select * from delivery_base_wkly_region_mo
)
, sales_base_wkly_groc as (
SELECT
  dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date::varchar as type,
  pl.sales_region_id,
  'Company Total' as sales_region_title,
  0.023 AS weeebates_pct,
  sum(daily_new_users_flag) as new_user,
  count(distinct buyer_id) as active_user,
  sum(sub_total) as gross_rev,
  count(distinct group_invoice_id) as delivery_cnt,
  round(gross_rev/active_user, 2) as arpu,
  round(gross_rev/delivery_cnt, 2) as rev_per_delivery,
  sum(coupon) as total_coupon_rev,
--   sum(new_user_coupon) as new_user_coupon_rev,
  -- total_coupon_rev - new_user_coupon_rev as coupon_rev,
  total_coupon_rev - 10*new_user as coupon_rev,
   coupon_rev/gross_rev as coupon_pct,
  -- max(weeebates_pct) as weee_bates_pct,
  -- coupon_pct+max(weeebates_pct) as discount_pct,
  coupon_pct+0.023 AS discount_pct,
  sum(oos) as oos_rev,
  oos_rev/gross_rev as oos_pct,
--   sum(refund) as refund_rev,
  case when type::date = dateadd('week', -1,date_trunc('week',current_date)-1) then sum(refund)/0.88 else sum(refund) end as refund_rev,
  refund_rev/gross_rev as refund_pct,
  oos_pct+refund_pct as oos_refund_pct,
  -- gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * max(weeebates_pct) as net_rev,
  gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * 0.023 as net_rev,
  net_rev/gross_rev as net_rev_pct,
  sum(cogs) as cogs_rev,
  sum(cogs)/gross_rev as cogs_pct,
  max(inbound_pct) as inbound_pct,
  (net_rev_pct-cogs_pct-max(inbound_pct)*net_rev_pct)/net_rev_pct as gm_pct,
  case when max(date_trunc('month', type::date)) = date_trunc('month', current_date) then max(packaging_pct_avg) else max(packaging_pct) end as packaging_pct,
  max(processing_fee_pct) as processing_fee_pct,
  max(cs_pct) as cs_pct,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_wh_dollar_value) else max(wh_pct) end as wh_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_wh_dollar_value)/net_rev
      else max(wh_pct) end as wh_pct,
--   case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_misc_dollar_value) else max(misc_pct) end as misc_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(misc_pct_avg)
      else max(misc_pct) end as misc_pct,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_ops_dollar_value) else max(ops_pct) end as ops_dollar,
  case when max(date_trunc('month',type::date)) = date_trunc('month',current_date) then  max(avg_ops_dollar_value)/net_rev
      else max(ops_pct) end as ops_pct,
  -- max(wh_pct) as wh_pct,
  -- max(ops_pct) as ops_pct,
  -- max(misc_pct) as misc_pct,
  '' as placeholder
from groc_base pl
left join (select * from region_pl where region_id = 999) rpl on pl.sales_region_id = rpl.region_id
where dateadd('day', -date_part(dow, delivery_day::date)::int, delivery_day::date)::date between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1))
group by 1,2,3,4
)
,delivery_base_wkly_groc_nomo as (
select  delivery_week::varchar AS type,
        999 as sales_region_id,
        SUM(total_delivery_cost)+SUM(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   delivery_week between date(dateadd('week', -4,date(date_trunc('week', current_date)-1))) AND date(dateadd('week', -1,date_trunc('week', current_date)-1))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_wkly_groc_mo as (
    select type,
           999 as sales_region_id,
           sum(delivery_cost) as delivery_cost
    from delivery_base_wkly_region_mo
    group by 1,2
)
, delivery_base_wkly_groc as (
    select type, sales_region_id, sum(delivery_cost) as delivery_cost from (select * from delivery_base_wkly_groc_nomo union all select * from delivery_base_wkly_groc_mo) group by 1,2
)
-- calculate labor cost
, p_and_l_labor as (
    select * from sandbox.ggs_nj_labor where date(delivery_week) < date_trunc('week', current_date)::date - 1
    union all
    select * from sandbox.ggs_chi_labor where date(delivery_week) < date_trunc('week', current_date)::date - 1
)
,  adp_tbl  as ( -- exclude DC cost and la greater
    select  entry_date
    , case when  warehouse = 'FL - Tampa' then 23
    when  warehouse = 'TX - Houston' then 10
    when warehouse = 'WA - Seattle' then 4
    when warehouse = 'IL - Chicago' then 15
    when warehouse = 'LA - La Mirada' then 2
    when warehouse = 'NJ - Edison' then 7
    when warehouse = 'SF - Union City' then 1 end as sales_region_id
    , sum(labor_cost) as total_labor_cost
    , sum(case when sales_region_id = 2 and lower(department_name) like '%dry%' then labor_cost else 0 end) as total_dry_labor_cost
    , sum(case when sales_region_id in (15,7) and lower(department_name) like '%mail order - outbound%' then labor_cost else 0 end ) as mo_outbound
    , case when sales_region_id = 2 then total_labor_cost - total_dry_labor_cost else 0 end as total_nodry_labor_cost
    from metrics.adp_labor_cost_detail
    where entry_week between date(dateadd('week', -4, date(date_trunc('week', current_date)-1))) and date(dateadd('week', -1, date_trunc('week', current_date)-1))
     and warehouse != 'check' and warehouse != 'LA - Greater'
    and filter_flag = 'N'
    group by 1,2
)
-- chicago and new jersy , mo rev
, chi_nj_base_tbl as (
select  delivery_day
    , case when inventory_title = 'IL - Chicago' then 15 else 7 end as sales_region_id
    , sum(case when biz_type like '%pantry%' or sales_region_id = 3 or lower(sales_region_title) like '%mof%'then sub_total else 0 end) as mo_rev
    , sum(sub_total) as total_rev
from metrics.order_product
where delivery_week between date(dateadd('week', -4, date(date_trunc('week', current_date)-1))) and date(dateadd('week', -1, date_trunc('week', current_date)-1))
and order_biz_type in ('grocery','alcohol')
and inventory_title in ('IL - Chicago','NJ - New York')
group by 1,2
)
, chi_nj as (
select delivery_day
    , sales_region_id
    , mo_rev / total_rev as mo_pct
from chi_nj_base_tbl
)
,la_base_by_region as
(select delivery_day
    , case when sales_region_id = 1 then 1
           when sales_region_id = 2 then 2
      else 3 end as sales_region_id
    , sum(case when division = 'Dry Grocery' then sub_total else 0 end) as dry_gross_rev
    , sum(case when division != 'Dry Grocery' then sub_total else 0 end) as non_dry_gross_rev
from metrics.order_product
where delivery_week between date(dateadd('week', -4, date(date_trunc('week', current_date)-1))) and date(dateadd('week', -1, date_trunc('week', current_date)-1))
and inventory_title = 'LA - La Mirada'
and order_biz_type in ('grocery','alcohol')
group by 1,2
)
, la_base as (
    select delivery_day
        , sum(dry_gross_rev) as dry_gross_rev
        , sum(non_dry_gross_rev) as non_dry_gross_rev
    from la_base_by_region
    group by 1
)
, la as (
    select labr.delivery_day
        , labr.sales_region_id
        , labr.dry_gross_rev / lab.dry_gross_rev as dry_rev_pct
        , labr.non_dry_gross_rev / lab.non_dry_gross_rev as non_dry_rev_pct
    from la_base_by_region labr
    left join la_base lab on labr.delivery_day = lab.delivery_day
)
, labor_base as (
    select entry_date
        , adp.sales_region_id
        , adp.total_labor_cost
        , total_dry_labor_cost
        , total_nodry_labor_cost
        , case when adp.sales_region_id = 2 then total_dry_labor_cost * la.dry_rev_pct else mo_outbound end as mo_outbound
        , case when adp.sales_region_id = 2 then total_dry_labor_cost * la1.dry_rev_pct else 0 end as dry_labor_cost --> la mirada to ba dry labor (should minus from la mirada
        , case when adp.sales_region_id = 2 then total_nodry_labor_cost * la1.non_dry_rev_pct else 0 end as nodry_labor_cost --> start from 10/15
        , case when adp.sales_region_id in (15,7) then adp.total_labor_cost * pll.inbound_pct::float * cn.mo_pct  else 0 end as mo_labor
    from adp_tbl  adp
    left join la on adp.entry_date = la.delivery_day and la.sales_region_id = 3
    left join la la1 on adp.entry_date = la1.delivery_day and la1.sales_region_id = 1
    left join p_and_l_labor pll on case when date_part(dayofweek, adp.entry_date) = 0 then adp.entry_date else  dateadd('day', -1, date_trunc('week', adp.entry_date))::date end = pll.delivery_week::date
               and pll.sales_region_id = adp.sales_region_id
    left join chi_nj cn on cn.delivery_day = adp.entry_date and cn.sales_region_id = adp.sales_region_id
)
, labor_cost as (
    select t1.entry_date
        , t1.sales_region_id
        , case when t1.sales_region_id not in (2,1,15,7) then t1.total_labor_cost
               when t1.sales_region_id = 2 then
                    case when t1.entry_date < '2022-10-15' then t1.total_dry_labor_cost - t1.mo_outbound - t1.dry_labor_cost + t1.total_nodry_labor_cost
                    when t1.entry_date >= '2022-10-15' then t1.total_dry_labor_cost - t1.mo_outbound - t2.dry_labor_cost + t1.total_nodry_labor_cost - t2.nodry_labor_cost end
               when t1.sales_region_id in (15,7) then t1.total_labor_cost - t1.mo_outbound - t1.mo_labor
               when t1.sales_region_id = 1 then
                    case when t1.entry_date < '2022-10-15' then t1.total_labor_cost + t2.dry_labor_cost
                         when t1.entry_date >='2022-10-15' then t1.total_labor_cost + t2.dry_labor_cost + t2.nodry_labor_cost end end as final_labor_cost
    from labor_base t1
    left join labor_base t2 on t1.entry_date = t2.entry_date and t2.sales_region_id = 2
)
, adp_labor_nomo as (
    select dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date as entry_week
        , sales_region_id
        , sum(final_labor_cost) as final_labor_cost
    from labor_cost
    group by 1,2
)
, adp_labor_mo as (
    select dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date as entry_week
        , 3 as sales_region_id
        , sum(mo_outbound)+sum(mo_labor) as final_labor_cost
    from labor_base
    group by 1,2
)
, adp_wkly_region as (
    select * from adp_labor_nomo union all select * from adp_labor_mo
)
, adp_wkly_groc as (
    select dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date as entry_week
        , 999 as sales_region_id
        , sum(total_labor_cost) as final_labor_cost
    from adp_tbl group by 1,2
)
,pl_wkly_region as (
  select  sb.*,
          db.delivery_cost*1.01 as delivery_cost,
          db.delivery_cost*1.01/sb.net_rev as delivery_pct,
          adp.final_labor_cost,
          adp.final_labor_cost/sb.net_rev as labor_cost_pct,
          '' as placeholder
  from    sales_base_wkly_region sb
  left join delivery_base_wkly_region db
  on      sb.sales_region_id = db.sales_region_id AND sb.type = db.type
  left join adp_wkly_region adp on adp.sales_region_id= sb.sales_region_id and sb.type = adp.entry_week::varchar
)
, pl_wkly_groc as (
  select  sb.*,
          db.delivery_cost*1.01 as delivery_cost,
          db.delivery_cost*1.01/sb.net_rev as delivery_pct,
          adp.final_labor_cost,
          adp.final_labor_cost/sb.net_rev as labor_cost_pct,
          '' as placeholder
  from    sales_base_wkly_groc sb
  left join delivery_base_wkly_groc db
  on      sb.sales_region_id = db.sales_region_id AND sb.type = db.type
  left join adp_wkly_groc  adp on adp.sales_region_id = sb.sales_region_id and sb.type = adp.entry_week::varchar
)
, sales_base_mthly_region as (
SELECT
  'Reported '+date_trunc('month',delivery_day)::date::varchar as type,
  sales_region_id_new as sales_region_id,
  sales_region_title_new as sales_region_title,
  0.023 AS weeebates_pct,
  sum(daily_new_users_flag) as new_user,
  count(distinct buyer_id) as active_user,
  sum(sub_total) as gross_rev,
  count(distinct group_invoice_id) as delivery_cnt,
  round(gross_rev/active_user, 2) as arpu,
  round(gross_rev/delivery_cnt, 2) as rev_per_delivery,
  sum(coupon) as total_coupon_rev,
--   sum(new_user_coupon) as new_user_coupon_rev,
  -- total_coupon_rev - new_user_coupon_rev as coupon_rev,
  total_coupon_rev - 10*new_user as coupon_rev,
  coupon_rev/gross_rev as coupon_pct,
  coupon_pct+0.023 AS discount_pct,
  sum(oos) as oos_rev,
  oos_rev/gross_rev as oos_pct,
  sum(refund) as refund_rev,
  refund_rev/gross_rev as refund_pct,
  oos_pct+refund_pct as oos_refund_pct,
  gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * 0.023 as net_rev,
  net_rev/gross_rev as net_rev_pct,
  sum(cogs) as cogs_rev,
  sum(cogs)/gross_rev as cogs_pct,
  max(inbound_pct) as inbound_pct,
  (net_rev_pct-cogs_pct-max(inbound_pct)*net_rev_pct)/net_rev_pct as gm_pct,
  max(packaging_pct) as packaging_pct,
  max(processing_fee_pct) as processing_fee_pct,
  max(cs_pct) as cs_pct,
  max(wh_pct) as wh_pct,
  max(misc_pct) as misc_pct,
  max(ops_pct) as ops_pct,
  max(wh_labor_pct) as wh_labor_pct,
  '' as placeholder
from region_base pl
left join (select * from region_pl where region_id != 999) rpl on pl.sales_region_id_new = rpl.region_id
where date_trunc('month',delivery_day)::date = dateadd('month', -1, date_trunc('month', current_date))::date
group by 1,2,3,4
)
, delivery_base_mthly_region_nomo as (
select  'Reported '+date_trunc('month',delivery_date)::date::varchar AS type,
        sales_region_id,
        sum(total_delivery_cost)+sum(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   date_trunc('month',delivery_date)::date = dateadd('month', -1,date_trunc('month', current_date))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_mthly_region_mo as (
select  'Reported '+delivery_month::varchar AS type,
        3 as sales_region_id,
        sum(total_delivery_cost) as delivery_cost
        from (
            select date(date_trunc('month',deal_delivery_date::date)) as delivery_month
            , wms_order_id
            , max(rate) as total_delivery_cost
            from metrics.mail_order_shipment
            where delivery_month = dateadd('month', -1,date_trunc('month', current_date))::date
            group by 1,2
        ) mo
group by 1,2
)
, delivery_base_mthly_region as (
    select * from delivery_base_mthly_region_nomo union all select * from delivery_base_mthly_region_mo
)
, sales_base_mthly_groc as (
SELECT
  'Reported '+date_trunc('month',delivery_day)::date::varchar as type,
  sales_region_id,
  'Company Total' as sales_region_title,
  0.023 AS weeebates_pct,
  sum(daily_new_users_flag) as new_user,
  count(distinct buyer_id) as active_user,
  sum(sub_total) as gross_rev,
  count(distinct group_invoice_id) as delivery_cnt,
  round(gross_rev/active_user, 2) as arpu,
  round(gross_rev/delivery_cnt, 2) as rev_per_delivery,
  sum(coupon) as total_coupon_rev,
  total_coupon_rev - 10*new_user as coupon_rev,
  coupon_rev/gross_rev as coupon_pct,
  coupon_pct+0.023 AS discount_pct,
  sum(oos) as oos_rev,
  oos_rev/gross_rev as oos_pct,
  sum(refund) as refund_rev,
  refund_rev/gross_rev as refund_pct,
  oos_pct+refund_pct as oos_refund_pct,
  gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev * 0.023 as net_rev,
  net_rev/gross_rev as net_rev_pct,
  sum(cogs) as cogs_rev,
  sum(cogs)/gross_rev as cogs_pct,
  max(inbound_pct) as inbound_pct,
  (net_rev_pct-cogs_pct-max(inbound_pct)*net_rev_pct)/net_rev_pct as gm_pct,
  max(packaging_pct) as packaging_pct,
  max(processing_fee_pct) as processing_fee_pct,
  max(cs_pct) as cs_pct,
  max(wh_pct) as wh_pct,
  max(misc_pct) as misc_pct,
  max(ops_pct) as ops_pct,
  max(wh_labor_pct) as wh_labor_pct,
  '' as placeholder
from groc_base pl
left join (select * from region_pl where region_id = 999) rpl on pl.sales_region_id = rpl.region_id
where date_trunc('month',delivery_day)::date = dateadd('month', -1, date_trunc('month', current_date))::date
group by 1,2,3,4
)
, delivery_base_mthly_groc_nomo as (
select  'Reported '+date_trunc('month',delivery_date)::date::varchar AS type,
        999 as sales_region_id,
        sum(total_delivery_cost)+sum(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   date_trunc('month',delivery_date)::date = dateadd('month', -1,date_trunc('month', current_date))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_mthly_groc_mo as (
select  'Reported '+delivery_month::varchar AS type,
        999 as sales_region_id,
        sum(total_delivery_cost) as delivery_cost
        from (
            select date(date_trunc('month',deal_delivery_date::date)) as delivery_month
            , wms_order_id
            , max(rate) as total_delivery_cost
            from metrics.mail_order_shipment
            where delivery_month = dateadd('month', -1,date_trunc('month', current_date))::date
            group by 1,2
        ) mo
group by 1,2
)
, delivery_base_mthly_groc as (
    select type, sales_region_id, sum(delivery_cost) as delivery_cost from (select * from delivery_base_mthly_groc_nomo union all select * from delivery_base_mthly_groc_mo) group by 1,2
)
, pl_mthly_region as (
    select sb.type
        , sb.sales_region_id
        , sb.sales_region_title
        , sb.weeebates_pct
        , sb.new_user
        , sb.active_user
        , sb.gross_rev
        , sb.delivery_cnt
        , sb.arpu
        , sb.rev_per_delivery
        , sb.total_coupon_rev
        , sb.coupon_rev
        , sb.coupon_pct
        , sb.discount_pct
        , sb.oos_rev
        , sb.oos_pct
        , sb.refund_rev
        , sb.refund_pct
        , sb.oos_refund_pct
        , sb.net_rev
        , sb.net_rev_pct
        , sb.cogs_rev
        , sb.cogs_pct
        , sb.inbound_pct
        , sb.gm_pct
        , 0 as final_labor_cost
        , sb.wh_labor_pct as  labor_cost_pct
        , packaging_pct
        , processing_fee_pct
        , 0.001 as cs_pct
        , 0 as wh_dollar
        , wh_pct
        , misc_pct
        ,  0 as ops_dollar
        , ops_pct
        , db.delivery_cost*1.01 as delivery_cost
        , db.delivery_cost*1.01 / sb.net_rev as delivery_pct
    from sales_base_mthly_region sb
    left join delivery_base_mthly_region db on sb.type = db.type and sb.sales_region_id = db.sales_region_id
)
, pl_mthly_groc as (
    select sb.type
        , sb.sales_region_id
        , sb.sales_region_title
        , sb.weeebates_pct
        , sb.new_user
        , sb.active_user
        , sb.gross_rev
        , sb.delivery_cnt
        , sb.arpu
        , sb.rev_per_delivery
        , sb.total_coupon_rev
        , sb.coupon_rev
        , sb.coupon_pct
        , sb.discount_pct
        , sb.oos_rev
        , sb.oos_pct
        , sb.refund_rev
        , sb.refund_pct
        , sb.oos_refund_pct
        , sb.net_rev
        , sb.net_rev_pct
        , sb.cogs_rev
        , sb.cogs_pct
        , sb.inbound_pct
        , sb.gm_pct
        , 0 as final_labor_cost
        , sb.wh_labor_pct as  labor_cost_pct
        , packaging_pct
        , processing_fee_pct
        , 0.001 as cs_pct
        , 0 as wh_dollar
        , wh_pct
        , misc_pct
        ,  0 as ops_dollar
        , ops_pct
        , db.delivery_cost*1.01 as delivery_cost
        , db.delivery_cost*1.01 / sb.net_rev as delivery_pct
    from sales_base_mthly_groc sb
    left join delivery_base_mthly_groc db on sb.type = db.type and sb.sales_region_id = db.sales_region_id
)
, tbl as
(select * from pl_wkly_region union all select * from pl_wkly_groc)
, t1 as
(select type
    , sales_region_id
    , sales_region_title
    , weeebates_pct
    , new_user
    , active_user
    , gross_rev
    , delivery_cnt
    , arpu
    , rev_per_delivery
    , total_coupon_rev
    , coupon_rev
    , coupon_pct
    , discount_pct
    , oos_rev
    , oos_pct
    , refund_rev
    , refund_pct
    , oos_refund_pct
    , net_rev
    , net_rev_pct
    , cogs_rev
    , cogs_pct
    , inbound_pct
    , gm_pct
    , case when type::date = dateadd('week', -1, date_trunc('week', current_date)-1)::date then lag(final_labor_cost) over (partition by sales_region_id  order by type::date ) else final_labor_cost end as final_labor_cost
    , case when type::date  = dateadd('week', -1,date_trunc('week', current_date)-1)::date then lag(labor_cost_pct) over (partition by sales_region_id order by type::date ) else labor_cost_pct end as labor_cost_pct
    , packaging_pct
    , processing_fee_pct
    , 0.001 as cs_pct
    , wh_dollar
    , wh_pct
    , misc_pct
    , ops_dollar
    , ops_pct
    , case when type::date = dateadd('week', -1,date_trunc('week', current_date)-1)::date then lag(delivery_cost) over (partition by sales_region_id order by type::date ) else delivery_cost end as delivery_cost
    , case when type::date = dateadd('week', -1,date_trunc('week', current_date)-1)::date then lag(delivery_pct) over (partition by sales_region_id order by type::date ) else delivery_pct end as delivery_pct
from tbl
order by 2,type::date)
, final_tbl as
(select * from t1
union all select * from pl_mthly_region
union all select * from pl_mthly_groc)
,lcs as
(select entry_date,
case when  warehouse = 'FL - Tampa' then 23
    when  warehouse = 'TX - Houston' then 10
    when warehouse = 'IL - Chicago' then 15
    when warehouse in  ('LA - La Mirada','LA - Greater') then 2
    when warehouse = 'NJ - Edison' then 7 end as sales_region_id ,
    sum(labor_cost) / nullif(sum(total_working_hours), 0)::float as hourly_rate
from metrics.adp_labor_cost_summary lcs
    where dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date
    between date(dateadd('week', -4,date(date_trunc('week', current_date)-1)))
    and date(dateadd('week', -1,date_trunc('week', current_date)-1))
    and warehouse not in ('SF - Union City', 'WA - Seattle')
group by 1,2)
, dws as (
select date(operation_date) as operation_day,
    case when inventory_title = 'FL - Tampa' then 23
    when inventory_title = 'TX - Houston' then 10
    when inventory_title = 'IL - Chicago' then 15
    when inventory_title in  ('LA - La Mirada','LA - Greater') then 2
    when inventory_title in('NJ - Edison','NJ - New York') then 7 end as sales_region_id ,
    sum(work_time) / 3600                                                   as total_hours
from  dws.wms_summary_detail dws
    where dateadd('day', -date_part(dow, operation_date::date)::int, operation_date::date)::date
    between date(dateadd('week', -4,date(date_trunc('week', current_date)-1)))
    and date(dateadd('week', -1,date_trunc('week', current_date)-1))
    and lower(operation_type) like '%repack%'
    and lower(split_part(inventory_title,'-',1)) not like '%sf%'
    and lower(split_part(inventory_title,'-',1)) not like '%wa%'
group by 1,2
)
, rlc_a as
(select dateadd('day', -date_part(dow, operation_day::date)::int, operation_day::date)::date as delivery_week
    , dws.sales_region_id
    , sum(total_hours * lcs.hourly_rate) as repack_labor_cost
from dws
left join lcs on lcs.entry_date = dws.operation_day and lcs.sales_region_id = dws.sales_region_id
group by 1,2 )
, rlc_b as (
select dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date as delivery_week,
    case when lower(warehouse) like '%sf%' THEN 1
    when lower(split_part(warehouse,'-',1))  like '%wa%' then 4 end as sales_region_id ,
    sum(labor_cost) as repack_labor_cost
from metrics.adp_labor_cost_detail
    where dateadd('day', -date_part(dow, entry_date::date)::int, entry_date::date)::date
    between date(dateadd('week', -4,date(date_trunc('week', current_date)-1)))
    and date(dateadd('week', -1,date_trunc('week', current_date)-1))
    and filter_flag = 'N'
    and department in ('OP5102', 'OP7499')
    and (lower(warehouse) like '%wa%' or lower(warehouse) like '%sf%')
    group by 1, 2
)
, rlc as (
    select * from rlc_a union all select * from rlc_b
)
select ft.*
    , nvl(rlc.repack_labor_cost,0) as repack_labor_cost
from final_tbl ft
left join rlc on ft.type = rlc.delivery_week::varchar and ft.sales_region_id = rlc.sales_region_id;

