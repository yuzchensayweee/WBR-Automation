SET TIME ZONE 'America/Los_Angeles';
-- Monthly Logic
with region_base as (
    select delivery_month
        , case when sales_region_id = 24  or lower(biz_type) like '%pantry%' then 3 --> mof to mo
               else sales_region_id end as sales_region_id_new
        , case when sales_region_id = 24 or lower(biz_type) like '%pantry%' then 'MAIL ORDER'
          else sales_region_title end as sales_region_title_new
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
        ,'' as place_holder
    from metrics.order_product
    where delivery_month = dateadd('month', -1, date_trunc('month', current_date))
    and order_biz_type in ('grocery','alcohol')
    group by 1,2,3,4,5,6
)
-- Monthly Net Rev
, region_rev as (
    select  delivery_month
        , sales_region_id_new as sales_region_id
        , sales_region_title_new as sales_region_title
        , 0.023 as weeebates
        , sum(daily_new_users_flag) as new_user
        , count(distinct buyer_id) as active_user
        , sum(sub_total) as gross_rev
        , sum(coupon) as total_coupon_rev
        , sum(new_user_coupon) as new_user_coupon
        , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , sum(refund) as refund_rev
        , sum(oos) as oos_rev
        , sum(cogs) as cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 as net_rev
    from region_base
    group by 1,2,3,4
)
-- Calculate Company Total Sales Base
, groc_rev as (
    select delivery_month
        , 999 as sales_region_id
        , 'Company' as sales_region_title
        , 0.023 as weeebates
        , sum(daily_new_users_flag) as new_user
        , count(distinct buyer_id) as active_user
        , sum(sub_total) as gross_rev
        , sum(coupon) as total_coupon_rev
        , sum(new_user_coupon) as new_user_coupon
        , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , sum(refund) as refund_rev
        , sum(oos) as oos_rev
        , sum(cogs) as cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 as net_rev
    from region_base
    group by 1,2,3,4
)
-- Calculate Reported P&L
,  reported_pl as (
select *
from (select delivery_month::date as report_month,
       dense_rank()over(order by report_month desc) as rn,
       case when lower(sales_region_title) = 'bay area' then 1
            when lower(sales_region_title) in ('consolidated','total' )then 999
            when lower(sales_region_title) in ('chicago','midwest') then 15
            when lower(sales_region_title) in ('east coast','new jersey' )then 7
            when lower(sales_region_title) = 'florida' then 23
            when lower(sales_region_title) = 'mail order' then 3
            when lower(sales_region_title) in ('northwest','seattle' )then 4
            when lower(sales_region_title) = 'texas' then 10
            when lower(sales_region_title) = 'la' then 2
       else null end as region_id
      , abs(inbound_pct::float) AS inbound_pct
      , abs(wh_labor_pct::float) as wh_labor
      , abs(packaging_pct::float) as packaging_pct
      , abs(processing_pct::float) as processing_fee_pct
      , 0.001 as css
      , abs(wh_pct::float) as wh_pct
      , abs(ops_pct::float) as ops_pct
      , abs(misc_pct::float) as misc_pct
from sandbox.ggs_monthly_actual_pl
where gross_rev != '' and region_id is not null
and report_month::date between dateadd('month', -2, date_trunc('month', current_date)) and dateadd('month', -1, date_trunc('month', current_date)) )
where rn = 1
)
, sales_base_region as (
    select  rr.*
        , inbound_pct*rr.net_rev as inbound_rev
        , processing_fee_pct*rr.net_rev as processing_rev
        , packaging_pct*rr.net_rev as packaging_rev
        , 0.001 * rr.net_rev as cs_rev
        , wh_pct*net_rev as warehouse_rev
        , misc_pct*net_rev as misc_rev
        , ops_pct*net_rev as ops_rev
        , '' as place_holder
    from region_rev rr
    left join (select * from reported_pl where region_id != 999) pl on rr.sales_region_id = pl.region_id
)
, sales_base_groc as (
    select  gr.*
        , inbound_pct*gr.net_rev as inbound_rev
        , processing_fee_pct*gr.net_rev as processing_rev
        , packaging_pct*gr.net_rev as packaging_rev
        , 0.001 * gr.net_rev as cs_rev
        , wh_pct*net_rev as warehouse_rev
        , misc_pct*net_rev as misc_rev
        , ops_pct*net_rev as ops_rev
        , '' as place_holder
    from groc_rev gr
    left join (select * from reported_pl where region_id = 999) pl on gr.sales_region_id = pl.region_id
)
,delivery_base_region_nomo as (
select  date_trunc('month',delivery_date)::date as delivery_month_2,
        sales_region_id,
        sum(total_delivery_cost)+sum(weee_linehaul_cost) AS delivery_cost
from    metrics.dispatch_performance dp
where   delivery_month_2  = dateadd('month', -1, date_trunc('month', current_date))
and     delivery_plan_type = 'grocery'
group by 1,2
)
, delivery_base_region_mo as (
select  delivery_month_2,
        3 as sales_region_id,
        sum(total_delivery_cost) as delivery_cost
        from (
            select date_trunc('month',deal_delivery_date::date)::date as delivery_month_2
            , wms_order_id
            , max(rate) as total_delivery_cost
            from metrics.mail_order_shipment
            where  delivery_month_2  = dateadd('month', -1, date_trunc('month', current_date))
            group by 1,2
        ) mo
group by 1,2
)
, delivery_base_region as (
select * from delivery_base_region_nomo union all select * from delivery_base_region_mo
)
, delivery_base_groc as (
select delivery_month_2, 999 as sales_region_id, sum(delivery_cost) as delivery_cost from delivery_base_region group by 1,2)
-- Separate Line for Labor Cost
, p_and_l_labor as (
    select delivery_week::date
         , sales_region_id
         , inbound_pct::float
         , lag(inbound_pct::float) over (partition by sales_region_id order by delivery_week::date ) as lag_inbound_pct
    from (select * from sandbox.ggs_nj_labor where delivery_week::date < date_trunc('week', current_date) union all select * from sandbox.ggs_chi_labor where delivery_week::date < date_trunc('week', current_date))
)
, adp_tbl  as (-- Exclude LA - Greater
    select  entry_date
    , entry_week
    , case when  warehouse = 'FL - Tampa' then 23
    when  warehouse = 'TX - Houston' then 10
    when warehouse = 'WA - Seattle' then 4
    when warehouse = 'IL - Chicago' then 15
    when warehouse = 'LA - La Mirada' then 2
    when warehouse = 'NJ - Edison' then 7
    when warehouse = 'SF - Union City' then 1 end as sales_region_id
    , sum(labor_cost) as total_cost
    , sum(case when sales_region_id = 2 and lower(department_name) like '%dry%' then labor_cost else 0 end) as dry_cost
    , case when sales_region_id = 2 then total_cost - dry_cost else 0 end as nondry_cost
    , sum(case when sales_region_id in (15,7) and lower(department_name) like '%mail order - outbound%' then labor_cost else 0 end ) as mo_outbound
    from metrics.adp_labor_cost_detail
    where date_trunc('month', entry_date) = dateadd('month', -1,date_trunc('month', current_date))
    and warehouse != 'check' and warehouse != 'LA - Greater'
    and filter_flag = 'N'
    group by 1,2,3
)
,dry_rev as
(select delivery_day
    , case when sales_region_id in (1,2) then sales_region_id else 3 end as sales_region_id
    , sum(sub_total) as dry_rev
from metrics.order_product
where delivery_month = dateadd('month', -1,date_trunc('month', current_date))
and inventory_title = 'LA - La Mirada'
and order_biz_type in ('grocery','alcohol')
and division = 'Dry Grocery'
group by 1,2
)
, total_dry as (select delivery_day , sum(dry_rev) as total_dry_rev from dry_rev group by 1)
, la_dry as (
    select labr.delivery_day
        , labr.sales_region_id
        , labr.dry_rev / lab.total_dry_rev as  dry_rev_pct
    from dry_rev labr
    left join total_dry  lab on labr.delivery_day = lab.delivery_day
)
, nondry_rev as (
select delivery_day
    , sales_region_id
    , sum(sub_total) as nondry_rev
from metrics.order_product
where delivery_month = dateadd('month', -1,date_trunc('month', current_date))
and inventory_title = 'LA - La Mirada'
and order_biz_type in ('grocery','alcohol')
and division != 'Dry Grocery'
and sales_region_id in (1,2)
group by 1,2
)
, total_nondry as (select delivery_day, sum(nondry_rev) as total_nondry_rev from nondry_rev group by 1)
, la_nondry as (
    select labr.delivery_day
        , labr.sales_region_id
        , labr.nondry_rev / lab.total_nondry_rev as  nondry_rev_pct
    from nondry_rev labr
    left join total_nondry lab on labr.delivery_day = lab.delivery_day
)
, la as (select lad.*
              , case when lad.sales_region_id = 2 and lad.delivery_day < '2022-10-15' then 1
                     when lad.sales_region_id = 1 and lad.delivery_day < '2022-10-15' then 0
                else nvl(land.nondry_rev_pct,0) end  as nondry_rev_pct
from la_dry lad left join la_nondry land on lad.delivery_day = land.delivery_day and lad.sales_region_id = land.sales_region_id )
, cn_tbl as (
select  delivery_day
    , case when inventory_title = 'IL - Chicago' then 15 else 7 end as sales_region_id
    , sum(case when biz_type like '%pantry%' or sales_region_id = 3 or lower(sales_region_title) like '%mof%'then sub_total else 0 end) as mo_rev
    , sum(sub_total) as total_rev
from metrics.order_product
where delivery_month = dateadd('month', -1, date_trunc('month', current_date))::date
and order_biz_type in ('grocery','alcohol')
and inventory_title in ('IL - Chicago','NJ - New York')
group by 1,2
)
, cn  as (
select delivery_day
    , sales_region_id
    , mo_rev / total_rev as mo_pct
from cn_tbl
)
, labor_base as (
    select entry_date
        , entry_week
        , adp.sales_region_id
        , adp.total_cost
        , dry_cost
        , nondry_cost
        , case when adp.sales_region_id = 2 then dry_cost * la.dry_rev_pct else mo_outbound end as mo_outbound
        , case when adp.sales_region_id in (15,7) then total_cost * nvl(pl.inbound_pct, lag_inbound_pct) * cn.mo_pct else 0 end as mo_labor
        , case when adp.sales_region_id = 2 then dry_cost * la1.dry_rev_pct else 0 end as allo_dry_cost --> la mirada to ba dry labor (should minus from la mirada
        , case when adp.sales_region_id = 2 then nondry_cost * la1.nondry_rev_pct else 0 end as allo_nondry_cost  --> start from 10/15
    from adp_tbl  adp
    left join la on adp.entry_date = la.delivery_day and la.sales_region_id = 3
    left join la la1 on adp.entry_date = la1.delivery_day and la1.sales_region_id = 1
    left join cn on adp.entry_date = cn.delivery_day and adp.sales_region_id = cn.sales_region_id
    left join p_and_l_labor pl on adp.entry_week = date(pl.delivery_week) and adp.sales_region_id = pl.sales_region_id
)
, labor_cost_nomo as (
    select t1.entry_date
        , t1.sales_region_id
        , t1.total_cost
        , nvl(lag.labor_cost,0) as la_greater_labor_cost
        , case when t1.sales_region_id = 1 then t2.allo_dry_cost else t1.allo_dry_cost end as allo_dry_cost
        , case when t1.sales_region_id = 1 then t2.allo_nondry_cost else t1.allo_nondry_cost end as allo_nondry_cost
        , t1.mo_outbound
        , t1.mo_labor
        , case when t1.sales_region_id not in (2,1,15,7) then t1.total_cost
               when t1.sales_region_id = 2 -- LA (Total labor cost - BA allocation - MO allocation + LA- Greater labor cost)
               then t1.dry_cost + t1.nondry_cost - t1.mo_outbound - t1.allo_dry_cost - t1.allo_nondry_cost + la_greater_labor_cost
               when t1.sales_region_id = 1 -- BA (Total labor cost + BA allocation)
               then t1.total_cost + t2.allo_dry_cost + t2.allo_nondry_cost
               when t1.sales_region_id in (15,7) then t1.total_cost - t1.mo_outbound - t1.mo_labor
                end as final_labor_cost
    from labor_base t1
    left join labor_base t2 on t1.entry_date = t2.entry_date and t2.sales_region_id = 2
    left join (select entry_date, sum(labor_cost) as labor_cost from metrics.adp_labor_cost_detail where filter_flag = 'N' and warehouse = 'LA - Greater' group by 1) lag
       on t1.sales_region_id = 2 and t1.entry_date = lag.entry_date )
, labor_cost_mo as
    (select entry_date
          , 3 as sales_region_id
          , sum(mo_outbound) + sum(mo_labor) as total_cost
          , 0 as la_greater_labor_cost
          , 0 as allo_dry_cost
          , 0 as allo_nondry_cost
          , sum(mo_outbound) as mo_outbound
          , sum(mo_labor) as mo_labor
          , sum(mo_outbound)+sum(mo_labor) as final_labor_cost
    from labor_cost_nomo
    group by 1,2,4,5,6)
,  adp_cost_region as
(select date_trunc('month',entry_date)::date as delivery_month_3
, sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_dry_cost) as allo_dry_cost
, sum(allo_nondry_cost) as allo_nondry_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from labor_cost_nomo
group by 1,2
union all
select date_trunc('month', entry_date)::date as delivery_month_3
, sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_dry_cost) as allo_dry_cost
, sum(allo_nondry_cost) as allo_nondry_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from labor_cost_mo
group by 1,2 )
, adp_cost_groc as (select delivery_month_3
, 999 as sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_dry_cost) as allo_dry_cost
, sum(allo_nondry_cost) as allo_nondry_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from adp_cost_region
group by 1,2
)
, pl_region as
(select * from sales_base_region sb
left join delivery_base_region db on sb.delivery_month = db.delivery_month_2 and sb.sales_region_id = db.sales_region_id
left join adp_cost_region adp on sb.delivery_month = adp.delivery_month_3 and db.sales_region_id = adp.sales_region_id)
, pl_groc as (
select * from sales_base_groc sb
left join delivery_base_groc db on sb.delivery_month = db.delivery_month_2 and sb.sales_region_id = db.sales_region_id
left join adp_cost_groc adp on sb.delivery_month = adp.delivery_month_3 and db.sales_region_id = adp.sales_region_id)
select * from pl_region union all select * from pl_groc
order by 2;