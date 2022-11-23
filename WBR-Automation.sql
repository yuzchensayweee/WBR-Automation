set time zone 'America/Los_Angeles';
with pp_reason as (
select distinct product_id,
       sales_org_id,
       reason,
       CONVERT_TIMEZONE ( 'America/Los_Angeles', timestamp 'epoch' + pps.start_time * interval '1 second' )  start_time_new,
       CONVERT_TIMEZONE ( 'America/Los_Angeles', timestamp 'epoch' + pps.end_time * interval '1 second' )   end_time_new,
       start_time_new -  interval '300 second' as start_time_extended,
       end_time_new + interval '900 second' as end_time_extended
from weee_p01.gb_product_price pp
left join weee_p01.gb_product_price_special pps on pps.product_price_id = pp.id
 where (date(end_time_extended) >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or date(end_time_extended) >= date_trunc('week', current_date)::date-1 - 7*55)
)
,
base1 as (
select case when {{WMQ}} = 'week' then op.delivery_week
            when {{WMQ}} = 'month' then op.delivery_month
            when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day::timestamp)::date
            else null end as delivery_unit_b1,
       ethnicity_user,
       sum(sub_total) as gross_rev,
       sum(gm_margin)/NULLIF(sum(gm_base),0) as prod_gm,
       sum(gm_margin) as gm_margin,
       sum(gm_base) as gm_base,
       count(distinct buyer_id) as active_users,
       sum(case when division = 'Produce'     then sub_total else 0 end) as eth_produce_rev, -- fruits & vegetables
       sum(case when division = 'Protein'     then sub_total else 0 end) as eth_protein_rev,
       sum(case when division = 'Frozen'      then sub_total else 0 end) as eth_frozen_rev,
       sum(case when division = 'Dairy/Deli'  then sub_total else 0 end) as eth_dairy_rev,
       sum(case when division = 'Dry Grocery' then sub_total else 0 end) as eth_dry_rev,
       sum(case when division = 'GM & HBC'    then sub_total else 0 end) as eth_gm_rev,
       count(distinct case when daily_new_users_flag = 1 then buyer_id else null end) as new_users,
       count(distinct case when op.sales_region_title = 'SF Bay Area'  and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_sf,
       count(distinct case when op.sales_region_title = 'LA - Greater' and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_la,
       count(distinct case when op.sales_region_title = 'New York'     and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_ny,
       count(distinct case when op.sales_region_title = 'Seattle'      and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_sea,
       count(distinct case when op.sales_region_title = 'Houston'      and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_tx,
       count(distinct case when op.sales_region_title = 'Chicago'      and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_chi,
       count(distinct case when op.sales_region_title = 'Tampa'        and lower(biz_type) not like '%pantry%' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_fl,
       count(distinct case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') and daily_new_users_flag = 1 then buyer_id else null end) as new_users_mo,
       count(distinct case when op.sales_region_title = 'SF Bay Area'   and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_sf,
       count(distinct case when op.sales_region_title = 'LA - Greater'  and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_la,
       count(distinct case when op.sales_region_title = 'New York'      and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_ny,
       count(distinct case when op.sales_region_title = 'Seattle'       and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_sea,
       count(distinct case when op.sales_region_title = 'Houston'       and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_tx,
       count(distinct case when op.sales_region_title = 'Chicago'       and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_chi,
       count(distinct case when op.sales_region_title = 'Tampa'         and lower(biz_type) not like '%pantry%' then buyer_id else null end) as users_fl,
       count(distinct case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') then buyer_id else null end) as users_mo,
       count(distinct group_invoice_id) as delivery_cnt,
       count(distinct order_id) as total_orders,
       count(distinct case when is_gm = 0 then order_id else null end) as oos_orders,
       count(distinct case when op.sales_region_title = 'SF Bay Area'                and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_sf,
       count(distinct case when op.sales_region_title = 'SF Bay Area' and is_gm = 0  and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_sf,
       count(distinct case when op.sales_region_title = 'LA - Greater'               and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_la,
       count(distinct case when op.sales_region_title = 'LA - Greater' and is_gm = 0 and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_la,
       count(distinct case when op.sales_region_title = 'New York'               and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_ny,
       count(distinct case when op.sales_region_title = 'New York' and is_gm = 0 and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_ny,
       count(distinct case when op.sales_region_title = 'Seattle'                and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_sea,
       count(distinct case when op.sales_region_title = 'Seattle' and is_gm = 0  and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_sea,
       count(distinct case when op.sales_region_title = 'Houston'                and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_tx,
       count(distinct case when op.sales_region_title = 'Houston' and is_gm = 0  and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_tx,
       count(distinct case when op.sales_region_title = 'Chicago'                and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_chi,
       count(distinct case when op.sales_region_title = 'Chicago' and is_gm = 0  and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_chi,
       count(distinct case when op.sales_region_title = 'Tampa'                  and lower(biz_type) not like '%pantry%' then order_id else null end) as total_orders_fl,
       count(distinct case when op.sales_region_title = 'Tampa' and is_gm = 0    and lower(biz_type) not like '%pantry%' then order_id else null end) as oos_orders_fl,
       count(distinct case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%')  then order_id else null end) as total_orders_mo,
       count(distinct case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') and is_gm = 0 then order_id else null end) as oos_orders_mo,
       count(distinct case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') and is_gm*product_refund_amount > 0 then order_id end) as return_orders_mo,
       sum(case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') then product_refund_amount * is_gm end) as refund_amount_mo,
       sum(case when (op.sales_region_title in ('MAIL ORDER','MOF') or lower(biz_type) like '%pantry%') then sub_total end) as gross_rev_mo,
       count(distinct case when op.sales_region_title = 'Tampa' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_fl,
       sum(case when op.sales_region_title = 'Tampa' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_fl,
       sum(case when op.sales_region_title = 'Tampa' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_fl,
       count(distinct case when op.sales_region_title = 'Chicago' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_chi,
       sum(case when op.sales_region_title = 'Chicago' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_chi,
       sum(case when op.sales_region_title = 'Chicago' and lower(biz_type) not like '%pantry%'  then sub_total end) as gross_rev_chi,
       count(distinct case when op.sales_region_title = 'Houston' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_tx,
       sum(case when op.sales_region_title = 'Houston' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_tx,
       sum(case when op.sales_region_title = 'Houston' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_tx,
       count(distinct case when op.sales_region_title = 'Seattle' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_sea,
       sum(case when op.sales_region_title = 'Seattle' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_sea,
       sum(case when op.sales_region_title = 'Seattle' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_sea,
       count(distinct case when op.sales_region_title = 'New York' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_ny,
       sum(case when op.sales_region_title = 'New York' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_ny,
       sum(case when op.sales_region_title = 'New York' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_ny,
       count(distinct case when op.sales_region_title = 'LA - Greater' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_la,
       sum(case when op.sales_region_title = 'LA - Greater' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_la,
       sum(case when op.sales_region_title = 'LA - Greater' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_la,
       count(distinct case when op.sales_region_title = 'SF Bay Area' and lower(biz_type) not like '%pantry%' and is_gm*product_refund_amount > 0 then order_id end) as return_orders_sf,
       sum(case when op.sales_region_title = 'SF Bay Area' and lower(biz_type) not like '%pantry%' then product_refund_amount * is_gm end) as refund_amount_sf,
       sum(case when op.sales_region_title = 'SF Bay Area' and lower(biz_type) not like '%pantry%' then sub_total end) as gross_rev_sf,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason,  pr_full.reason, pr_full1.reason)  in('Vendor funded','Boost Revenue - Vendor fund') then (product_base_price-product_price)*quantity end) as vendor_funded_promo,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason,  pr_full.reason, pr_full1.reason)  = 'Boost Revenue' or nvl(discount_reason,  pr_full.reason, pr_full1.reason) is null or nvl(discount_reason,  pr_full.reason, pr_full1.reason)= '' then (product_base_price-product_price)*quantity end) as boost_rev_promo,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason,  pr_full.reason, pr_full1.reason)  = 'Clearance' then (product_base_price-product_price)*quantity end) as clearance_promo,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason,  pr_full.reason, pr_full1.reason)  = 'Expiring Control' then (product_base_price-product_price)*quantity end) as expiring_control_promo,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason, pr_full.reason, pr_full1.reason)  like '%Freeup space%' then (product_base_price-product_price)*quantity end) as freeup_space_promo,
       sum(case when price_type in ('lightening','sale') and nvl(discount_reason, pr_full.reason, pr_full1.reason)  like '%Vendor Sponsored Promotion%'then (product_base_price-product_price)*quantity  end) as vendor_sponsored_promo,
       sum(case when division = 'Produce'     then product_refund_amount * is_gm else 0 end) as eth_return_produce_rev, -- fruits & vegetables
       sum(case when division = 'Protein'     then product_refund_amount * is_gm else 0 end) as eth_return_protein_rev,
       sum(case when division = 'Frozen'      then product_refund_amount * is_gm else 0 end) as eth_return_frozen_rev,
       sum(case when division = 'Dairy/Deli'  then product_refund_amount * is_gm else 0 end) as eth_return_dairy_rev,
       sum(case when division = 'Dry Grocery' then product_refund_amount * is_gm else 0 end) as eth_return_dry_rev,
       sum(case when division = 'GM & HBC'    then product_refund_amount * is_gm else 0 end) as eth_return_gm_rev
from   metrics.order_product op
left   join metrics.sales_org_price_region_mapping soprm ON op.sales_org_id = soprm.sales_org_id
left join pp_reason pr_full
on     op.order_create_time >=  pr_full.start_time_extended
and    op.order_create_time <   pr_full.end_time_extended
and    op.product_id = pr_full.product_id
and    soprm.price_region_id = pr_full.sales_org_id  -- use org to join
    and op.discount_reason is null

left join pp_reason pr_full1
on     op.order_create_time >=  pr_full1.start_time_extended
and    op.order_create_time <   pr_full1.end_time_extended
and    op.product_id = pr_full1.product_id
and    case when op.sales_org_id in (17,18,20,23,15,21,27,28) then 16
            when op.sales_org_id in (4,8,10,13,14,19) then 3
            else null end = pr_full1.sales_org_id -- use inventory to join
and    op.mail_type = 'pantry'
and op.discount_reason is null

where  (delivery_unit_b1 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b1 = date_trunc('week', current_date)::date-1 - 7*53)
and    order_biz_type <> 'restaurant'
and    (order_day >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or order_day >= date_trunc('week', current_date)::date-1 - 7*55)
group by 1,2
order by 1,2
),
base2 as (
select
  case when {{WMQ}} = 'week' then delivery_week
       when {{WMQ}} = 'month' then delivery_month
       when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day::timestamp)::date
       else null end as delivery_unit_b2,
  sum(discount) AS discount,
  sum(expense_pct/100.0000*revenue) AS points_expense,
  sum(net_revenue) AS net_revenue,
  sum(new_user_coupon) AS new_user_coupon,
  sum(oos) AS oos,
  --sum(revenue) AS revenue,
  sum(case when {{WMQ}} = 'week' and delivery_week = dateadd('week', -1, DATEADD(d, - datepart(dow, current_date), current_date)::DATE)
  and date_diff('day', delivery_week, current_date) in (8,9) then shipped_and_refunded/0.88 else shipped_and_refunded end) as shipped_and_refunded,
  sum(total_discount) AS total_discount
from metrics.revenue_main_table_with_language
where  (delivery_unit_b2 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}})
or     delivery_unit_b2 = date_trunc('week', current_date)::date-1 - 7*53)
group by 1),
base3 as (
select case when {{WMQ}} = 'week'    then date_trunc('week',    sess_date+1)::date-1
            when {{WMQ}} = 'month'   then date_trunc('month',   sess_date)::date
            when {{WMQ}} = 'quarter' then date_trunc('quarter', sess_date)::date
            else null end  as delivery_unit_b3,
       sum(orders) as orders,
       count(distinct session_id) as total_sessions
from   metrics.sessions_details
where  (delivery_unit_b3 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b3 = date_trunc('week', current_date)::date-1 - 7*53)
and (sess_date >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or sess_date >= date_trunc('week', current_date)::date-1 - 7*55)
group  by 1
),
base4 as (
select  case when {{WMQ}} = 'week'    then date_trunc('week',   response_date+1)::date-1
            when {{WMQ}} = 'month'   then date_trunc('month', response_date)::date
            when {{WMQ}} = 'quarter' then date_trunc('quarter', response_date)::date
            else null end AS delivery_unit_b4,
        round((sum(promoter_flag)-sum(detractor_flag))*100.00/NULLIF(count(distinct response_id),0),0) as nps,
/*MOF - NJ*/
        round((sum(case when response_region = 'SF Bay Area' then promoter_flag else 0 end)
              -sum(case when response_region = 'SF Bay Area' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'SF Bay Area' then response_id else null end),0),0) as nps_sf,

        round((sum(case when response_region = 'LA - Greater' then promoter_flag else 0 end)
              -sum(case when response_region = 'LA - Greater' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'LA - Greater' then response_id else null end),0),0) as nps_la,

        round((sum(case when response_region = 'Seattle' then promoter_flag else 0 end)
              -sum(case when response_region = 'Seattle' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'Seattle' then response_id else null end),0),0) as nps_sea,

        round((sum(case when response_region = 'New York' then promoter_flag else 0 end)
              -sum(case when response_region = 'New York' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'New York' then response_id else null end),0),0) as nps_ny,

        round((sum(case when response_region = 'Houston' then promoter_flag else 0 end)
              -sum(case when response_region = 'Houston' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'Houston' then response_id else null end),0),0) as nps_tx,

        round((sum(case when response_region = 'Chicago' then promoter_flag else 0 end)
              -sum(case when response_region = 'Chicago' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'Chicago' then response_id else null end),0),0) as nps_chi,

        round((sum(case when response_region = 'Tampa' then promoter_flag else 0 end)
              -sum(case when response_region = 'Tampa' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'Tampa' then response_id else null end),0),0) as nps_fl,

        round((sum(case when response_region = 'MAIL ORDER - WEST' then promoter_flag else 0 end)
              -sum(case when response_region = 'MAIL ORDER - WEST' then detractor_flag else 0 end))*100.00
   /nullif(count(distinct case when response_region = 'MAIL ORDER - WEST' then response_id else null end),0),0) as nps_mo
from   metrics.survey_user_response_detail
where  (delivery_unit_b4 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b4 = date_trunc('week', current_date)::date-1 - 7*53)
and    question_id = 1
and    (response_date >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or response_date >= date_trunc('week', current_date)::date-1 - 7*55)
group by 1
),
base5 as (
select  case when {{WMQ}} = 'week'    then date_trunc('week',   delivery_day+1)::date-1
             when {{WMQ}} = 'month'   then date_trunc('month', delivery_day)::date
             when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day)::date
             else null end as delivery_unit_b5,
        1-(sum(not_perfect_orders)*1.00/NULLIF(sum(total_orders),0)) as perfect_order_rate,
        1-(sum(case when region = '1 - SF Bay Area' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '1 - SF Bay Area' then total_orders else 0 end),0)) as perfect_order_rate_sf,
        1-(sum(case when region = '2 - LA - Greater' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '2 - LA - Greater' then total_orders else 0 end),0)) as perfect_order_rate_la,
        1-(sum(case when region = '4 - Seattle' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '4 - Seattle' then total_orders else 0 end),0)) as perfect_order_rate_sea,
        1-(sum(case when region = '7 - New York' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '7 - New York' then total_orders else 0 end),0)) as perfect_order_rate_ny,
        1-(sum(case when region = '10 - Houston' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '10 - Houston' then total_orders else 0 end),0)) as perfect_order_rate_tx,
        1-(sum(case when region = '15 - Chicago' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '15 - Chicago' then total_orders else 0 end),0)) as perfect_order_rate_chi,
        1-(sum(case when region = '23 - Tampa' then not_perfect_orders else 0 end)*1.00
          /nullif(sum(case when region = '23 - Tampa' then total_orders else 0 end),0)) as perfect_order_rate_fl
from   metrics.main_dashboard_perfect_order_rate
where  (delivery_unit_b5 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b5 = date_trunc('week', current_date)::date-1 - 7*53)
and    delivery_type = 'grocery'
group  by 1
),
new_booking as (
select order_day as time_day_unit,
       order_week as time_week_unit,
       case when device_type = 'APP iOS' then 'iOS'
            when device_type = 'APP Android' then 'Android'
            else 'Web' END AS Device,
       case when language = 'zh' then 'Chinese'
            when language = 'zh-Hant' then 'Chinese'
            when language = 'en' then 'English'
            else 'OTHER' end as Language,
       case when sales_org_id in (3,16) then 'Mail'
            else 'Grocery' end AS Region,
       count(distinct buyer_id) as new_d2c
from metrics.order
where payment_mode = 'F'
      AND daily_new_users_flag_order = 1
      AND order_day between '2020-12-27' and dateadd('day',-1,current_date)::DATE
GROUP BY 1,2,3,4,5),
new_revenue as (
select delivery_day as time_day_unit,delivery_week as time_week_unit,
       case when device_type = 'APP iOS' THEN 'iOS'
            when device_type = 'APP Android' THEN 'Android'
            else 'Web' end AS Device,
       case when language = 'zh' THEN 'Chinese'
            when language = 'zh-Hant' THEN 'Chinese'
            when language = 'en' THEN 'English'
            else 'OTHER' end AS Language,
       case when sales_org_id IN (3,16) THEN 'Mail'
            else 'Grocery' end AS Region,
       count(distinct buyer_id) as new_d2c
FROM metrics.order
WHERE payment_mode = 'F'
      AND daily_new_users_flag = 1
      AND delivery_day between '2020-12-27' and dateadd('day',-1,current_date)::DATE
GROUP BY 1,2,3,4,5
),

templtv as (
select device,language,region,wd,case when region = 'Local' then 0.1*avg_ltv else 0.05*avg_ltv end as avg_ltv
from
(select device,language,region,wd,sum(revenue)/nullif(sum(cohort_buyers), 0) as avg_ltv
from metrics.mkt_ltv
where start_week <= dateadd(week,-7, dateadd(day,-(CAST(date_part(dw,current_date) AS INT))+1,dateadd(day,-1,current_date)))::Date
and wd <=6
group by 1,2,3,4)),

LTV AS (
select a.device,a.language,case when a.region = 'Local' then 'Grocery' else a.region end as region ,1.0*first_6_week+45*avg_ltv AS ltv
from
(select  device,language,region,sum(avg_ltv) as first_6_week
from templtv
group by 1,2,3) a inner join
(select device,language,region,avg_ltv from templtv where wd=6) b on a.device=b.device and a.language=b.language and a.region=b.region
),

temp1 as (
select a.time_day_unit,a.time_week_unit,
       sum(a.booking_new_d2c) AS Booking_new_user,
       sum(a.revenue_new_d2c) AS Revenue_new_user,
       sum(a.booking_new_d2c*b.ltv) as total_booking_ltv,
       sum(a.revenue_new_d2c*b.ltv) as total_revenue_ltv
from
(select coalesce(a.time_day_unit,b.time_day_unit) as time_day_unit,
       coalesce(a.time_week_unit,b.time_week_unit) as time_week_unit,
       coalesce(a.Region,b.Region) as Region,
       coalesce(a.Device,b.Device) as Device,
       coalesce(a.Language,b.Language) as Language,
       isnull(a.new_d2c,0) as booking_new_d2c,
       isnull(b.new_d2c,0) as revenue_new_d2c
from new_booking a
    FULL OUTER JOIN new_revenue b on a.time_day_unit=b.time_day_unit
                                  and a.time_week_unit=b.time_week_unit
                                  and a.Region=b.Region
                                  and a.Device=b.Device
                                  and a.Language=b.Language) a
    LEFT JOIN LTV b on a.Region=b.Region
                    and a.Device=b.Device
                    and a.Language=b.Language
group by 1,2),

marketing_spend as (
select date as time_day_unit,
       dateadd(day,-(CAST(date_part(dw,date) AS INT)),date)::Date AS time_week_unit,
       sum(cost) as marketing_spend
from metrics.marketing_spend
where channel not like 'Branding%'
group by 1,2),

coupon_spend as (
select coalesce(a.time_day_unit,b.time_day_unit) as time_day_unit,
       coalesce(a.time_week_unit,b.time_week_unit) as time_week_unit,
       new_user_book_coupon_amount,
       new_user_rev_coupon_amount
from
(select order_day  AS time_day_unit,
        order_week as time_week_unit,
    sum(od.amount) AS new_user_book_coupon_amount
from metrics.order o
left join weee_p01.gb_coupon_use c ON o.order_id = c.order_id
left join weee_p01.gb_coupon_code cc ON c.code = cc.code
left join weee_p01.gb_order_discount od ON od.order_id = o.order_id
where order_day >='2020-12-27'
and payment_mode = 'F'
and o.discount > 0
and od.type = 'coupon'
and (o.coupon = 'sign_up_coupon' or o.coupon = 'order_share_sign_up_coupon')
group by 1,2) a

full outer join

(select delivery_day  AS time_day_unit,
       delivery_week as time_week_unit,
       sum(od.amount) AS new_user_rev_coupon_amount
from metrics.order o
left join weee_p01.gb_coupon_use c ON o.order_id = c.order_id
left join weee_p01.gb_coupon_code cc ON c.code = cc.code
left join weee_p01.gb_order_discount od ON od.order_id = o.order_id
where delivery_day >='2020-12-27'
and payment_mode = 'F'
and o.discount > 0
and od.type = 'coupon'
and (o.coupon = 'sign_up_coupon' or o.coupon = 'order_share_sign_up_coupon')
group by 1,2) b
    on a.time_day_unit=b.time_day_unit and a.time_week_unit=b.time_week_unit),

referral_spend as (
    select CONVERT_TIMEZONE ('America/Los_Angeles', rec_create_time)::DATE as time_day_unit,
           dateadd(day,-(CAST(date_part(dw,CONVERT_TIMEZONE ('America/Los_Angeles', rec_create_time)) AS INT)),CONVERT_TIMEZONE ('America/Los_Angeles', rec_create_time))::Date AS time_week_unit,
           sum(points/100) as referral_cost
    from weee_p01.user_points
    where type = 'p_user_referral'
    and CONVERT_TIMEZONE ('America/Los_Angeles', rec_create_time)::DATE >='2020-12-27'
    group by 1,2
),

other_spend as (
    select a.order_day as time_day_unit,
           a.order_week as time_week_unit,
           sum(round(isnull(b.content_marketing_spend,0),2)) as content_marketing_spend,
           sum(isnull(salary_cost,0)) as salary_cost,
           sum(isnull(agency_fee,0)) as agency_fee,
           sum(isnull(contractor_fee,0)) as contractor_fee,
           sum(isnull(tools_cost,0)) as tools_cost,
           sum(isnull(c.brand_cost,0)) as branding_cost,
           sum(isnull(b.es_ethnic_cost,0)+isnull(b.ja_ethnic_cost,0)+isnull(b.ko_ethnic_cost,0)+isnull(b.vi_ethnic_cost,0)) as ethnic_cost,
           sum(round(isnull(b.content_marketing_spend,0),2)+isnull(b.salary_cost,0)+isnull(b.agency_fee,0)+isnull(b.contractor_fee,0)+isnull(b.tools_cost,0)+isnull(c.brand_cost,0)+isnull(b.es_ethnic_cost,0)+isnull(b.ja_ethnic_cost,0)+isnull(b.ko_ethnic_cost,0)+isnull(b.vi_ethnic_cost,0)) as total_other_spend
from
(select order_month,order_day,order_week
from metrics.order
where payment_mode = 'F'
      and order_month >= '2020-12-01' and order_day >='2020-12-01'
group by 1,2,3) a
join
(
select * from metrics.mkt_other_spend
where month>='2020-12-01'
) b on a.order_month = b.month
left join
(select date as time_day_unit,sum(cost) as brand_cost
from metrics.marketing_spend
where channel like 'Branding%'
group by 1) c on a.order_day = c.time_day_unit
group by 1,2
),
base6 as (
select    case when {{WMQ}} = 'week'    then date_trunc('week',   a.time_day_unit+1)::date-1
               when {{WMQ}} = 'month'   then date_trunc('month', a.time_day_unit)::date
               when {{WMQ}} = 'quarter' then date_trunc('quarter', a.time_day_unit)::date
               else null end  as delivery_unit_b6,
           sum(revenue_new_user) as revenue_new_user,
           sum(isnull(marketing_spend,0)) as marketing_spend,
           sum(isnull(new_user_rev_coupon_amount,0)) as  new_user_rev_coupon_amount,
           sum(isnull(referral_cost,0)) as referral_spend,
           sum(isnull(content_marketing_spend,0)) as content_marketing_spend,
           sum(isnull(salary_cost,0)) as salary_spend,
           sum(isnull(agency_fee,0)) as agency_spend,
           sum(isnull(contractor_fee,0)) as contractor_spend,
           sum(isnull(tools_cost,0)) as tool_spend,
           sum(isnull(marketing_spend,0)) +
           sum(isnull(new_user_rev_coupon_amount,0)) +
           sum(isnull(referral_cost,0)) +
           sum(isnull(content_marketing_spend,0)) +
           sum(isnull(salary_cost,0)) +
           sum(isnull(agency_fee,0)) +
           sum(isnull(contractor_fee,0)) +
           sum(isnull(tools_cost,0)) as total_spend
from temp1 a
left join coupon_spend b on a.time_day_unit = b.time_day_unit and a.time_week_unit = b.time_week_unit
left join marketing_spend c on a.time_day_unit = c.time_day_unit and a.time_week_unit = c.time_week_unit
left join referral_spend d on a.time_day_unit = d.time_day_unit and a.time_week_unit = d.time_week_unit
left join other_spend e on a.time_day_unit = e.time_day_unit and a.time_week_unit = e.time_week_unit
where  (delivery_unit_b6 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b6 = date_trunc('week', current_date)::date-1 - 7*53)
group by 1
),
base7 as (
select  case when {{WMQ}} = 'week'    then date_trunc('week',   delivery_day+1)::date-1
             when {{WMQ}} = 'month'   then date_trunc('month', delivery_day)::date
             when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day)::date
             else null end as delivery_unit_b7,
        sum(case when order_type = 'RTG' then order_total else 0 end) as sc_booking,
        sum(case when order_type = 'RTG' then cm_margin else 0 end)/nullif(sum(case when order_type = 'RTG' then cm_base else 0 end),0) as sc_cm,
        sum(case when order_type in ('WeeeOD', 'Ricepo', 'Ricepo_vip_order') then order_total else 0 end) as od_booking,
        sum(case when order_type in ('WeeeOD', 'Ricepo', 'Ricepo_vip_order') then cm_margin else 0 end)/nullif(sum(case when order_type in ('WeeeOD', 'Ricepo') then cm_base else 0 end),0) as od_cm,
        sum(order_total) as sc_od_booking,
        sum(cm_margin)/nullif(sum(cm_base),0) as sc_od_cm
FROM   dws.restaurant_order
where  (delivery_unit_b7 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b7 = date_trunc('week', current_date)::date-1 - 7*53)
and    (order_day >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or order_day >= date_trunc('week', current_date)::date-1 - 7*55)
group by 1
),
base8 as (
select  case when {{WMQ}} = 'week'    then delivery_week
             when {{WMQ}} = 'month'   then delivery_month
             when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day)::date
             else null end as delivery_unit_b8,
        sum(case when            division = 'Produce' then sub_total else 0 end)   as produce_rev,
        count(distinct case when division = 'Produce' then buyer_id else null end) as produce_user,
        sum(case when   division = 'Produce' then gm_margin else null end)
       /nullif(sum( case when   division = 'Produce' then gm_base else null end),0)  as produce_gm,

        sum(case when            division = 'Protein' then sub_total else 0 end)   as protein_rev,
        count(distinct case when division = 'Protein' then buyer_id else null end) as protein_user,
        sum(case when   division = 'Protein' then gm_margin else null end)
       /nullif(sum(case when   division = 'Protein' then gm_base else null end),0)  as protein_gm,

        sum(case when            division = 'Frozen' then sub_total else 0 end)   as frozen_rev,
        count(distinct case when division = 'Frozen' then buyer_id else null end) as frozen_user,
        sum(case when   division = 'Frozen' then gm_margin else null end)
       /nullif(sum(case when   division = 'Frozen' then gm_base else null end),0)  as frozen_gm,

        sum(case when            division = 'Dairy/Deli' then sub_total else 0 end)   as dairy_rev,
        count(distinct case when division = 'Dairy/Deli' then buyer_id else null end) as dairy_user,
        sum(case when   division = 'Dairy/Deli' then gm_margin else null end)
       /nullif(sum(case when   division = 'Dairy/Deli' then gm_base else null end),0)  as dairy_gm,

        sum(case when            division = 'Dry Grocery' then sub_total else 0 end)   as dry_rev,
        count(distinct case when division = 'Dry Grocery' then buyer_id else null end) as dry_user,
        sum(case when   division = 'Dry Grocery' then gm_margin else null end)
       /nullif(sum(case when   division = 'Dry Grocery' then gm_base else null end),0)  as dry_gm,

        sum(case when            division = 'GM & HBC' then sub_total else 0 end)   as gm_rev,
        count(distinct case when division = 'GM & HBC' then buyer_id else null end) as gm_user,
        sum(case when   division = 'GM & HBC' then gm_margin else null end)
       /nullif(sum(case when   division = 'GM & HBC' then gm_base else null end),0)  as gm_gm
from   metrics.order_product
where  (delivery_unit_b8 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b8 = date_trunc('week', current_date)::date-1 - 7*53)
and    (order_day >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or order_day >= date_trunc('week', current_date)::date-1 - 7*55)
group by 1
)
, base9 AS (
  select DATE(delivery_unit)  as delivery_unit_b9
    , unit_type
    , gross_rev_plan
    , net_rev_plan
    , gm_plan as prod_gm_plan
    , new_users as new_user_plan
    , active_users as active_user_plan
    , arpu_plan
  from sandbox.ggs_wbr_plan_data
  where  (delivery_unit_b9 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b9 = date_trunc('week', current_date)::date-1 - 7*53)
)
, base11 AS (
select case when {{WMQ}} = 'week'    then delivery_week
             when {{WMQ}} = 'month'   then date_trunc('month', delivery_date)::date
             when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_date)::date
             else null end  AS delivery_unit_b11,
     count(distinct case when tms_on_time_flag = 'Y' and sales_region = '1 - SF Bay Area' then invoice_id END) AS on_time_deliveries_sf,
     count(distinct case when sales_region = '1 - SF Bay Area' THEN invoice_id end) as total_deliveries_sf,
     count(distinct case when sales_region = '2 - LA - Greater' AND tms_on_time_flag = 'Y' then invoice_id end) as on_time_deliveries_la,
     count(distinct case when sales_region = '2 - LA - Greater' then invoice_id end) as total_deliveries_la,
     count(distinct case when sales_region = '7 - New York' AND tms_on_time_flag = 'Y' then invoice_id end) as on_time_deliveries_ny,
     count(distinct case when sales_region = '7 - New York' then invoice_id end) as total_deliveries_ny,
     count(distinct case when sales_region = '4 - Seattle' AND tms_on_time_flag = 'Y'then invoice_id end) as on_time_deliveries_sea,
     count(distinct case when sales_region = '4 - Seattle' then invoice_id end) as total_deliveries_sea,
     count(distinct case when sales_region = '10 - Houston' AND tms_on_time_flag ='Y' then invoice_id end) as on_time_deliveries_tx,
     count(distinct case when sales_region = '10 - Houston' then invoice_id end) as total_deliveries_tx,
     count(distinct case when sales_region = '15 - Chicago' AND tms_on_time_flag = 'Y' then invoice_id end) as on_time_deliveries_chi,
     count(distinct case when sales_region = '15 - Chicago' then invoice_id end) as total_deliveries_chi,
     count(distinct case when sales_region = '23 - Tampa' AND tms_on_time_flag = 'Y' then invoice_id end) as on_time_deliveries_fl,
     count(distinct case when sales_region = '23 - Tampa' then invoice_id end) as total_deliveries_fl
    FROM metrics.dispatch_performance
    WHERE (delivery_unit_b11 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
          delivery_unit_b11 = date_trunc('week', current_date)::date-1 - 7*53)
    and    (delivery_date >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or delivery_date >= date_trunc('week', current_date)::date-1 - 7*55)
    group by 1 )
, cal_base as
    (select case when {{WMQ}} = 'week' then week_begin_date
                 when {{WMQ}} = 'month' then month_begin_date
                 when {{WMQ}} = 'quarter' then quarter_begin_date end as delivery_unit_b12 ,
    count(distinct case when {{WMQ}} = 'week' then
         case when date_trunc('month',cal_date) = date_trunc('month',week_begin_date) then cal_date else null end
    else null end) as same_month,
    max(case when {{WMQ}} = 'week' then
         case when date_trunc('month',cal_date) != date_trunc('month',week_begin_date) then 1 else 0 end
    else 0 end) as flag
from weee_p01.calendar
where (week_begin_date between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
          week_begin_date = date_trunc('week', current_date)::date-1 - 7*53)
group by 1)
, gm_plan_t1 as
(select cb.delivery_unit_b12,
     (max(wbr1.dairy_deli) * max(same_month) + max(7-same_month) * max(wbr2.dairy_deli) )/7 as dairy_deli_plan,
     (max(wbr1.protein) * max(same_month) + max(7-same_month) * max(wbr2.protein ) )/7 as protein_plan,
    (max(wbr1.frozen) * max( same_month) + max(7-same_month) * max(wbr2.frozen) )/7 as frozen_plan,
    (max(wbr1.produce) * max( same_month) + max(7-same_month) * max(wbr2.produce ) )/7 as produce_plan,
       (max(wbr1.dry_grocery) * max( same_month) + max(7-same_month) * max(wbr2.dry_grocery ) )/7 as dry_grocery_plan,
       (max(wbr1.gm_hbc) * max(same_month) + max(7-same_month) * max(wbr2.gm_hbc) )/7 as gm_hbc_plan
from cal_base cb
left join sandbox.ggs_wbr_division_plan wbr1 on date(wbr1.delivery_month) = date(date_trunc('month',delivery_unit_b12))
    left join sandbox.ggs_wbr_division_plan wbr2 on date_add('month', 1, date(wbr2.delivery_month)) = date_trunc('month', delivery_unit_b12)
where flag = 1
group by 1)
, gm_plan_t2 as (
    select delivery_unit_b12,
       dairy_deli as dairy_deli_plan,
       protein as protein_plan,
       frozen as frozen_plan,
       produce as produce_plan,
       dry_grocery as dry_grocery_plan,
       gm_hbc as gm_hbc_plan
from cal_base cb
left join sandbox.ggs_wbr_division_plan wbr on date(wbr.delivery_month) = date(date_trunc('month',delivery_unit_b12))
where flag =0)
, gm_plan as (select * from gm_plan_t1 union all select * from gm_plan_t2 )
, base12 as (
    select delivery_unit_b12,
           dairy_deli_plan,
           protein_plan,
           frozen_plan,
           produce_plan,
           dry_grocery_plan,
           gm_hbc_plan
    from gm_plan
)
, base13 as (
     select case when {{WMQ}} = 'week'    then date_trunc('week',   delivery_day+1)::date-1
               when {{WMQ}} = 'month'   then date_trunc('month', delivery_day)::date
               when {{WMQ}} = 'quarter' then date_trunc('quarter', delivery_day)::date
               else null end  as delivery_unit_b13,
          count(distinct case when split_part(inventory_title,'-',1) = 'FL' then group_invoice_id end) as delivery_cnt_fl,
          count(distinct case when split_part(inventory_title,'-',1) = 'FL' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_fl,
          count(distinct case when split_part(inventory_title,'-',1) = 'IL' then group_invoice_id end) as delivery_cnt_chi,
          count(distinct case when split_part(inventory_title,'-',1) = 'IL' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_chi,
          count(distinct case when split_part(inventory_title,'-',1) = 'LA' then group_invoice_id end) as delivery_cnt_la,
          count(distinct case when split_part(inventory_title,'-',1) = 'LA' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_la,
          count(distinct case when split_part(inventory_title,'-',1) = 'NJ' then group_invoice_id end) as delivery_cnt_nj,
          count(distinct case when split_part(inventory_title,'-',1) = 'NJ' and(order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_nj,
          count(distinct case when split_part(inventory_title,'-',1) = 'SF' then group_invoice_id end) as delivery_cnt_sf,
          count(distinct case when split_part(inventory_title,'-',1) = 'SF' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_sf,
          count(distinct case when split_part(inventory_title,'-',1) = 'TX' then group_invoice_id end) as delivery_cnt_tx,
          count(distinct case when split_part(inventory_title,'-',1) = 'TX' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_tx,
          count(distinct case when split_part(inventory_title,'-',1) = 'WA' then group_invoice_id end) as delivery_cnt_sea,
          count(distinct case when split_part(inventory_title,'-',1) = 'WA' and (order_case_id_np is not null or order_case_id_p is not null) then group_invoice_id end) as delivery_case_sea
     from metrics.order_product_case_details
     where  ((delivery_unit_b13 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
        delivery_unit_b13 = date_trunc('week', current_date)::date-1 - 7*53)
and    (order_day >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or order_day >= date_trunc('week', current_date)::date-1 - 7*55))
group by 1
)
, base14 as (
          select case when {{WMQ}} = 'week'    then date_trunc('week',   pos.date+1)::date-1
               when {{WMQ}} = 'month'   then date_trunc('month', pos.date)::date
               when {{WMQ}} = 'quarter' then date_trunc('quarter', pos.date)::date
               else null end  as delivery_unit_b14,
               avg(case when storage_type = 'N' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%houston%'  then max_sku_count end) as dry_cap_tx,
               avg(case when storage_type = 'F' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%houston%' then max_sku_count end) as frozen_cap_tx,
               avg(case when storage_type = 'R' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%houston%' then max_sku_count end) as ref_cap_tx,
               avg(case when storage_type = 'N' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%chicago%'  then max_sku_count end) as dry_cap_chi,
               avg(case when storage_type = 'F' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%chicago%' then max_sku_count end) as frozen_cap_chi,
               avg(case when storage_type = 'R' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%chicago%' then max_sku_count end) as ref_cap_chi,
               avg(case when storage_type = 'N' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%tampa%' then max_sku_count end) as dry_cap_fl,
               avg(case when storage_type = 'F' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%tampa%' then max_sku_count end) as frozen_cap_fl,
               avg(case when storage_type = 'R' and split_part(pos.title, '_', 3) = '' and lower(pos.title) like '%tampa%' then max_sku_count end) as ref_cap_fl
          from metrics.po_sku_params_snapshot pos
          where  ((delivery_unit_b14 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
          delivery_unit_b14 = date_trunc('week', current_date)::date-1 - 7*53)
          and (pos.date >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or pos.date >= date_trunc('week', current_date)::date-1 - 7*55))
          and (is_food = '-' and is_protein = '-' and is_produce = '-' and ethnicity = '-')
          group by 1 
)
, base15 as (
     select case when {{WMQ}} = 'week'    then date_trunc('week',   order_day+1)::date-1
               when {{WMQ}} = 'month'   then date_trunc('month', order_day)::date
               when {{WMQ}} = 'quarter' then date_trunc('quarter', order_day)::date
               else null end  as delivery_unit_b15,
               count(distinct case when storage_type = 'N' and lower(sales_region_title) like '%houston%' and split_part(department,'-',1) in ('05','09','11', '10','06','07', '15','14', '20','17', '16','12') then product_id end) as dry_instock_tx, -- dry grocery and gm & hbc
               count(distinct case when storage_type = 'F' and lower(sales_region_title) like '%houston%' and split_part(department,'-',1) = '08' then product_id end) as frozen_instock_tx, -- frozen only 
               count(distinct case when storage_type = 'R' and lower(sales_region_title) like '%houston%' and split_part(department,'-',1) = '13' then product_id end) as ref_instock_tx, -- dairy/deli
               count(distinct case when storage_type = 'N' and lower(sales_region_title) like '%chicago%' and split_part(department,'-',1) in ('05','09','11', '10','06','07', '15','14', '20','17', '16','12') then product_id end) as dry_instock_chi,
               count(distinct case when storage_type = 'F' and lower(sales_region_title) like '%chicago%' and split_part(department,'-',1) = '08' then product_id end) as frozen_instock_chi,
               count(distinct case when storage_type = 'R' and lower(sales_region_title) like '%chicago%' and split_part(department,'-',1) = '13' then product_id end) as ref_instock_chi,
               count(distinct case when storage_type = 'N' and lower(sales_region_title) like '%tampa%' and split_part(department,'-',1) in ('05','09','11', '10','06','07', '15','14', '20','17', '16','12') then product_id end) as dry_instock_fl,
               count(distinct case when storage_type = 'F' and lower(sales_region_title) like '%tampa%' and split_part(department,'-',1) = '08' then product_id end) as frozen_instock_fl,
               count(distinct case when storage_type = 'R' and lower(sales_region_title) like '%tampa%' and split_part(department,'-',1) = '13' then product_id end) as ref_instock_fl
     from metrics.in_stock_rate_data_source
    where  ((delivery_unit_b15 between dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) and dateadd({{WMQ}} ,-1, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) or
          delivery_unit_b15 = date_trunc('week', current_date)::date-1 - 7*53)
          and (order_day >= dateadd({{WMQ}} ,-14, date_trunc({{WMQ}},current_date)::date-{{WMQ1}}) - 20 or order_day >= date_trunc('week', current_date)::date-1 - 7*55)
          )
    and sku_region_status = 'A' and sku_availability = 1 
    group by 1
)
, base_all as (
select *
from      base1 b1
left join base2 b2
on b1.delivery_unit_b1 = b2.delivery_unit_b2
and b1.ethnicity_user = 'Chinese'
left join base3 b3
on b1.delivery_unit_b1 = b3.delivery_unit_b3
and b1.ethnicity_user = 'Chinese'
left join base4 b4
on b1.delivery_unit_b1 = b4.delivery_unit_b4
and b1.ethnicity_user = 'Chinese'
left join base5 b5
on b1.delivery_unit_b1 = b5.delivery_unit_b5
and b1.ethnicity_user = 'Chinese'
left join base6 b6
on b1.delivery_unit_b1 = b6.delivery_unit_b6
and b1.ethnicity_user = 'Chinese'
left join base7 b7
on b1.delivery_unit_b1 = b7.delivery_unit_b7
and b1.ethnicity_user = 'Chinese'
left join base8 b8
on b1.delivery_unit_b1 = b8.delivery_unit_b8
and b1.ethnicity_user = 'Chinese'
left join base9 b9
ON b1.delivery_unit_b1 = b9.delivery_unit_b9
and b1.ethnicity_user  = 'Chinese' and b9.unit_type = {{WMQ}}
left join base11 b11
on b1.delivery_unit_b1 = b11.delivery_unit_b11
and b1.ethnicity_user = 'Chinese'
left join base12 b12
on b1.delivery_unit_b1 = b12.delivery_unit_b12
and b1.ethnicity_user = 'Chinese'
left join base13 b13
on b1.delivery_unit_b1 = b13.delivery_unit_b13
and b1.ethnicity_user = 'Chinese'
left join base14 b14 
on b1.delivery_unit_b1 = b14.delivery_unit_b14 
and b1.ethnicity_user = 'Chinese'
left join base15 b15
on b1.delivery_unit_b1 = b15.delivery_unit_b15
and b1.ethnicity_user = 'Chinese'
)
select delivery_unit_b1::date as delivery_week, * from base_all
union all
select dateadd({{WMQ}} ,   -15, date_trunc({{WMQ}},    current_date)::date-{{WMQ1}})::date as delivery_week, * from base_all where delivery_unit_b1 = dateadd({{WMQ}} ,   -{{WMQ2}}, date_trunc({{WMQ}},    current_date)::date-{{WMQ1}}) and {{WMQ}} <> 'week'
order by 1,2,3;