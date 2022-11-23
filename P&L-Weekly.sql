set time zone 'America/Los_Angeles';
-- get weekly sales base data
with region_base as (
    select delivery_week
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
        , dense_rank() over (partition by date(date_trunc('month', delivery_week)) order by delivery_week desc) as rk
        ,'' as place_holder
    from metrics.order_product
    where delivery_week between date(dateadd('week', -9,dateadd(d, - datepart(dow, current_date), current_date)::date)) AND date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
    and order_biz_type in ('grocery','alcohol')
    group by 1,2,3,4,5,6
)
-- get regional weekly net rev
, region_rev as (
    select  delivery_week
        , rk
        , sales_region_id_new as sales_region_id
        , sales_region_title_new as sales_region_title
        , 0.023 as weeebates
        , sum(daily_new_users_flag) as new_user
        , count(distinct buyer_id) as active_user
        , sum(sub_total) as gross_rev
        , gross_rev/active_user as arpu
        , sum(coupon) as total_coupon_rev
        , sum(new_user_coupon) as new_user_coupon
        -- , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , total_coupon_rev - 10*new_user as coupon_rev
        , case when delivery_week = date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))
               and date_diff('day', delivery_week, current_date) = 8 then 
               case when sales_region_id_new = 3 then sum(refund)/0.7 else sum(refund)/0.9 end 
          else sum(refund) end as refund_rev
        , sum(oos) as oos_rev
        , oos_rev + refund_rev as total_refund_rev
        , sum(cogs) as cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 as net_rev
    from region_base
    group by 1,2,3,4,5
)
-- calculate company weekly net rev
, groc_rev as (
    select delivery_week
        , rk
        , 999 as sales_region_id
        , 'Company Total' as sales_region_title
        , 0.023 as weeebates
        , sum(daily_new_users_flag) as new_user
        , count(distinct buyer_id) as active_user
        , sum(sub_total) as gross_rev
        , gross_rev/active_user as arpu
        , sum(coupon) as total_coupon_rev
        , sum(new_user_coupon) as new_user_coupon
        -- , sum(coupon) - sum(new_user_coupon) as coupon_rev
        , total_coupon_rev - 10*new_user as coupon_rev
        , case when delivery_week = date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))
               and date_diff('day', delivery_week, current_date) = 8 then sum(refund)/0.9 else sum(refund) end as refund_rev
        , sum(oos) as oos_rev
        , oos_rev + refund_rev as total_refund_rev
        , sum(cogs) as cogs_rev
        , gross_rev - oos_rev - refund_rev - coupon_rev - gross_rev*0.023 as net_rev
    from region_base
    group by 1,2,3,4,5
)
-- get reported monthly P&L data
,  reported_pl_base as (
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
   -- packaging: previous 3 months avg. pct
   -- payment: previous month pct
   -- warehouse facility: use previous month weekly avg dollar value
   -- ops: previous month weekly avg dollar value
   -- misc: previous 2 month avg. pct
   -- inbound: previous month pct --> dollar value
, region_pl as (
  select pl.report_month
  , pl.region_id
  , max(pl.inbound_pct) as inbound_pct
  , max(pl.packaging_pct) as packaging_pct
  , avg(pl3.packaging_pct) as packaging_pct_avg
  , max(pl.processing_fee_pct) as processing_fee_pct
  , max(pl.wh_pct) as wh_pct
  , avg(pl.wh_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_wh_dollar_value
  , max(pl.ops_pct) as ops_pct
  , avg(pl.ops_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_ops_dollar_value
  , max(pl.misc_pct) as misc_pct
  , avg(pl.misc_pct * coalesce(gr.net_rev, rr.net_rev)) as avg_misc_dollar_value
from reported_pl pl
left join (select * from region_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date)))) ) rr on pl.region_id = rr.sales_region_id
left join (select * from groc_rev where date_trunc('month',delivery_week) = dateadd('month',-1,date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))))) gr on pl.region_id  = gr.sales_region_id
left join (select report_month, region_id, packaging_pct from reported_pl where rn <= 3 ) pl3 ON pl.region_id = pl3.region_id
where rn = 1
group by 1,2
)
, sales_base_wkly_region as (
    select  rr.*
        , inbound_pct*rr.net_rev as inbound_rev
        , processing_fee_pct*rr.net_rev as processing_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then packaging_pct_avg*rr.net_rev else packaging_pct*rr.net_rev end as packaging_rev
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
        , inbound_pct*gr.net_rev as inbound_rev
        , processing_fee_pct*gr.net_rev as processing_rev
        , case when date_trunc('month', delivery_week) = date_trunc('month', date(dateadd('week',-1,dateadd(d, - datepart(dow, current_date), current_date)::date))) then packaging_pct_avg*gr.net_rev else packaging_pct*gr.net_rev end as packaging_rev
        , 0.001 * gr.net_rev as cs_rev
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
,delivery_base_wkly_region_nomo as (
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
, p_and_l_labor as (
    select delivery_week::date
         , sales_region_id
         , inbound_pct::float
         , lag(inbound_pct::float) over (partition by sales_region_id order by delivery_week::date ) as lag_inbound_pct
    from (select * from sandbox.ggs_nj_labor where delivery_week::date < dateadd(d, - datepart(dow, current_date), current_date)::date union all select * from sandbox.ggs_chi_labor where delivery_week::date < dateadd(d, - datepart(dow, current_date), current_date)::date)
)
, adp_tbl  as (-- Exclude LA - Greater
    select case when entry_week = '2022-10-09' then entry_date else entry_week end as entry_week
    , case when  warehouse = 'FL - Tampa' then 23
    when  warehouse = 'TX - Houston' then 10
    when warehouse = 'WA - Seattle' then 4
    when warehouse = 'IL - Chicago' then 15
    when warehouse = 'LA - La Mirada' then 2
    when warehouse = 'NJ - Edison' then 7
    when warehouse = 'SF - Union City' then 1 end as sales_region_id
    , sum(labor_cost) as total_cost
    , sum(case when sales_region_id = 2 and department_name not like '%OP - DC%' then labor_cost else 0 end) as total_cost_ex_dc
    , sum(case when sales_region_id = 2 and lower(department_name) like '%dry%' then labor_cost else 0 end) as dry_cost
    , sum(case when sales_region_id in (15,7) and lower(department_name) like '%mail order - outbound%' then labor_cost else 0 end ) as mo_outbound
    from metrics.adp_labor_cost_detail
    where entry_week between date(dateadd('week', -4,dateadd(d, - datepart(dow, current_date), current_date)::date)) and date(dateadd('week', -1,dateadd(d, - datepart(dow, current_date), current_date)::date))
    and warehouse != 'check' and warehouse != 'LA - Greater'
    and filter_flag = 'N'
    group by 1,2
)
,la_base  as
(select case when delivery_week = '2022-10-09' then delivery_day else delivery_week end as delivery_week
    , case when sales_region_id in (1,2) then sales_region_id else 3 end as sales_region_id
    , sum(case when storage_type = 'N' then sub_total else 0 end) as dry_rev
    , sum(sub_total) as gross_rev
from metrics.order_product
where delivery_week  >= '2022-09-25'
and inventory_title = 'LA - La Mirada'
and order_biz_type in ('grocery','alcohol')
group by 1,2
)
, la as (select delivery_week , sum(dry_rev) as total_dry_rev, sum(gross_rev) as total_gross_rev from la_base group by 1)
, la_pct as (
    select labr.delivery_week
        , labr.sales_region_id
        , case when labr.sales_region_id = 1 and labr.delivery_week <= '2022-10-14' then labr.dry_rev / lab.total_dry_rev
               when labr.sales_region_id = 1 and labr.delivery_week >= '2022-10-15' then labr.gross_rev/lab.total_gross_rev
               when labr.sales_region_id = 3 then labr.dry_rev/lab.total_dry_rev else 0 end as rev_pct
    from la_base labr
    left join la  lab on labr.delivery_week = lab.delivery_week
)
, cn_tbl as (
select  case when delivery_week = '2022-10-09' then delivery_day else delivery_week end as delivery_week
    , case when inventory_title = 'IL - Chicago' then 15 else 7 end as sales_region_id
    , sum(case when biz_type like '%pantry%' or sales_region_id = 3 or lower(sales_region_title) like '%mof%'then sub_total else 0 end) as mo_rev
    , sum(sub_total) as total_rev
from metrics.order_product
where delivery_week >= '2022-09-25'
and order_biz_type in ('grocery','alcohol')
and inventory_title in ('IL - Chicago','NJ - New York')
group by 1,2
)
, cn  as (
select delivery_week
    , sales_region_id
    , mo_rev / total_rev as mo_pct
from cn_tbl
)
, labor_base as (
    select  entry_week
        , adp.sales_region_id
        , adp.total_cost
        , adp.total_cost_ex_dc
        , dry_cost
        , mo_outbound
        , case when adp.sales_region_id in (15,7) then total_cost * nvl(pl.inbound_pct, lag_inbound_pct) * cn.mo_pct
               when adp.sales_region_id = 2 then dry_cost * la.rev_pct else 0 end as mo_labor
        , case when adp.sales_region_id = 2 and adp.entry_week <= '2022-10-14' then adp.dry_cost * la1.rev_pct
               else adp.total_cost_ex_dc * la1.rev_pct end as allo_cost  --> LA - LA Mirada to Bay Area
    from adp_tbl  adp
    left join la_pct la on adp.entry_week = la.delivery_week and la.sales_region_id = 3
    left join la_pct la1 on adp.entry_week = la1.delivery_week and la1.sales_region_id = 1
    left join cn on adp.entry_week = cn.delivery_week and adp.sales_region_id = cn.sales_region_id
    left join p_and_l_labor pl on DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE = date(pl.delivery_week) and adp.sales_region_id = pl.sales_region_id
)
, labor_cost_nomo as (
    select t1.entry_week
        , t1.sales_region_id
        , t1.total_cost
        , nvl(lagt.labor_cost,0) as la_greater_labor_cost
        , case when t1.sales_region_id = 1 then t2.allo_cost else t1.allo_cost end as allo_cost
        , t1.mo_outbound
        , t1.mo_labor
        , case when t1.sales_region_id not in (2,1,15,7) then t1.total_cost
               when t1.sales_region_id = 2 -- LA (Total labor cost - BA allocation - MO allocation + LA- Greater labor cost)
               then t1.total_cost - t1.mo_labor - t1.allo_cost + la_greater_labor_cost
               when t1.sales_region_id = 1 -- BA (Total labor cost + BA allocation)
               then t1.total_cost + t2.allo_cost
               when t1.sales_region_id in (15,7) then t1.total_cost - t1.mo_outbound - t1.mo_labor
            end as final_labor_cost
    from labor_base t1
    left join labor_base t2 on t1.entry_week = t2.entry_week and t2.sales_region_id = 2
    left join (select  case when entry_week = '2022-10-09' then entry_date else entry_week end as entry_week, sum(labor_cost) as labor_cost from metrics.adp_labor_cost_detail where filter_flag = 'N' and warehouse = 'LA - Greater' group by 1) lagt
       on t1.sales_region_id = 2 and t1.entry_week = lagt.entry_week )
, labor_cost_mo as
    (select entry_week
          , 3 as sales_region_id
          , sum(mo_outbound) + sum(mo_labor) as total_cost
          , 0 as la_greater_labor_cost
          , 0 as allo_cost
          , sum(mo_outbound) as mo_outbound
          , sum(mo_labor) as mo_labor
          , sum(mo_outbound)+sum(mo_labor) as final_labor_cost
    from labor_cost_nomo
    group by 1,2,4,5)
, adp_cost_region as
( select case when entry_week between '2022-10-09' and '2022-10-15' then DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE else entry_week end as entry_week
, sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_cost) as allo_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from labor_cost_nomo
group by 1,2
union all
select case when entry_week between '2022-10-09' and '2022-10-15' then DATEADD(d, - datepart(dow, entry_week), entry_week)::DATE else entry_week end as entry_week
, sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_cost) as allo_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from labor_cost_mo
group by 1,2
    )
, adp_cost_groc as
(select  entry_week
, 999 as sales_region_id
, sum(total_cost) as total_cost
, sum(la_greater_labor_cost) as la_greater_labor_cost
, sum(allo_cost) as allo_cost
, sum(mo_outbound) as mo_outbound
, sum(mo_labor) as mo_labor
, sum(final_labor_cost) as final_labor_cost
from adp_cost_region
group by 1,2
)
, pl_region as
(select sb.*
, delivery_cost as delivery_cost
, delivery_cost*1.01 / net_rev as delivery_cost_pct
, adp.total_cost
, adp.la_greater_labor_cost
, adp.allo_cost
, adp.mo_outbound
, adp.mo_labor
, adp.final_labor_cost
, adp.final_labor_cost/net_rev as wh_labor_pct
 from sales_base_wkly_region sb
join delivery_base_wkly_region db on sb.delivery_week = db.delivery_week and sb.sales_region_id = db.sales_region_id
join adp_cost_region adp on adp.sales_region_id = sb.sales_region_id and sb.delivery_week = adp.entry_week )
, pl_groc as (
select sb.*
, delivery_cost as delivery_cost
, delivery_cost*1.01 / net_rev as delivery_cost_pct
, adp.total_cost
, adp.la_greater_labor_cost
, adp.allo_cost
, adp.mo_outbound
, adp.mo_labor
, adp.final_labor_cost
, adp.final_labor_cost/net_rev as wh_labor_pct
 from sales_base_wkly_groc sb
join delivery_base_wkly_groc db on sb.delivery_week = db.delivery_week and sb.sales_region_id = db.sales_region_id
join adp_cost_groc adp on adp.sales_region_id = sb.sales_region_id and sb.delivery_week = adp.entry_week)
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
    , total_cost
    , la_greater_labor_cost
    , allo_cost
    , mo_outbound
    , mo_labor
    , final_labor_cost as curr_week_labor_cost
    , case when delivery_week =  dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date then lag(final_labor_cost) over (partition by sales_region_id  order by delivery_week) else final_labor_cost end as final_labor_cost
    , case when delivery_week =  dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date then lag(delivery_cost) over (partition by sales_region_id  order by delivery_week) else delivery_cost end as delivery_cost
    , case when delivery_week =  dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date then lag(delivery_cost_pct) over (partition by sales_region_id  order by delivery_week) else delivery_cost_pct end as delivery_cost_pct
    , packaging_pct
    , case when delivery_week =  dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date then lag(processing_pct) over (partition by sales_region_id  order by delivery_week) else processing_pct end as processing_pct
    , case when delivery_week =  dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)::date then lag(wh_labor_pct) over (partition by sales_region_id  order by delivery_week) else wh_labor_pct end as wh_labor_pct
from pl_base
order by 2,1;