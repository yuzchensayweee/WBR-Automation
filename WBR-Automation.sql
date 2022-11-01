with base1 as (
select op.delivery_week,
       ethnicity_user,
       sum(sub_total) as gross_rev,
       sum(gm_margin)/sum(gm_base) as prod_gm,
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
       count(distinct case when sales_region_title = 'SF Bay Area'  and daily_new_users_flag = 1 then buyer_id else null end) as new_users_sf,
       count(distinct case when sales_region_title = 'LA - Greater' and daily_new_users_flag = 1 then buyer_id else null end) as new_users_la,
       count(distinct case when sales_region_title = 'New York'     and daily_new_users_flag = 1 then buyer_id else null end) as new_users_ny,
       count(distinct case when sales_region_title = 'Seattle'      and daily_new_users_flag = 1 then buyer_id else null end) as new_users_sea,
       count(distinct case when sales_region_title = 'Houston'      and daily_new_users_flag = 1 then buyer_id else null end) as new_users_tx,
       count(distinct case when sales_region_title = 'Chicago'      and daily_new_users_flag = 1 then buyer_id else null end) as new_users_chi,
       count(distinct case when sales_region_title = 'Tampa'        and daily_new_users_flag = 1 then buyer_id else null end) as new_users_fl,
       count(distinct case when sales_region_title = 'MAIL ORDER'   and daily_new_users_flag = 1 then buyer_id else null end) as new_users_mo,
       count(distinct case when sales_region_title = 'MOF'          and daily_new_users_flag = 1 then buyer_id else null end) as new_users_mof,
       count(distinct case when sales_region_title = 'SF Bay Area'   then buyer_id else null end) as users_sf,
       count(distinct case when sales_region_title = 'LA - Greater'  then buyer_id else null end) as users_la,
       count(distinct case when sales_region_title = 'New York'      then buyer_id else null end) as users_ny,
       count(distinct case when sales_region_title = 'Seattle'       then buyer_id else null end) as users_sea,
       count(distinct case when sales_region_title = 'Houston'       then buyer_id else null end) as users_tx,
       count(distinct case when sales_region_title = 'Chicago'       then buyer_id else null end) as users_chi,
       count(distinct case when sales_region_title = 'Tampa'         then buyer_id else null end) as users_fl,
       count(distinct case when sales_region_title = 'MAIL ORDER'    then buyer_id else null end) as users_mo,
       count(distinct case when sales_region_title = 'MOF'           then buyer_id else null end) as users_mof,
       count(distinct group_invoice_id) as delivery_cnt,
       count(distinct order_id) as total_orders,
       count(distinct case when is_gm = 0 then order_id else null end) as oos_orders,
       count(distinct case when sales_region_title = 'SF Bay Area'               then order_id else null end) as total_orders_sf,
       count(distinct case when sales_region_title = 'SF Bay Area' and is_gm = 0 then order_id else null end) as oos_orders_sf,
       count(distinct case when sales_region_title = 'LA - Greater'               then order_id else null end) as total_orders_la,
       count(distinct case when sales_region_title = 'LA - Greater' and is_gm = 0 then order_id else null end) as oos_orders_la,
       count(distinct case when sales_region_title = 'New York'               then order_id else null end) as total_orders_ny,
       count(distinct case when sales_region_title = 'New York' and is_gm = 0 then order_id else null end) as oos_orders_ny,
       count(distinct case when sales_region_title = 'Seattle'               then order_id else null end) as total_orders_sea,
       count(distinct case when sales_region_title = 'Seattle' and is_gm = 0 then order_id else null end) as oos_orders_sea,
       count(distinct case when sales_region_title = 'Houston'               then order_id else null end) as total_orders_tx,
       count(distinct case when sales_region_title = 'Houston' and is_gm = 0 then order_id else null end) as oos_orders_tx,
       count(distinct case when sales_region_title = 'Chicago'               then order_id else null end) as total_orders_chi,
       count(distinct case when sales_region_title = 'Chicago' and is_gm = 0 then order_id else null end) as oos_orders_chi,
       count(distinct case when sales_region_title = 'Tampa'               then order_id else null end) as total_orders_fl,
       count(distinct case when sales_region_title = 'Tampa' and is_gm = 0 then order_id else null end) as oos_orders_fl,
       count(distinct case when sales_region_title = 'MAIL ORDER'               then order_id else null end) as total_orders_mo,
       count(distinct case when sales_region_title = 'MAIL ORDER' and is_gm = 0 then order_id else null end) as oos_orders_mo,
       count(distinct case when sales_region_title = 'MOF'               then order_id else null end) as total_orders_mof,
       count(distinct case when sales_region_title = 'MOF' and is_gm = 0 then order_id else null end) as oos_orders_mof,
       count(distinct case when sales_region_title = 'MOF' then return_order_id end) as return_orders_mof,
       sum(case when sales_region_title = 'MOF' then refund_amount end) as refund_amount_mof,
       sum(case when sales_region_title = 'MOF' then sub_total end) as gross_rev_mof,
       count(distinct case when sales_region_title = 'MAIL ORDER' then return_order_id end) as return_orders_mo,
       sum(case when sales_region_title = 'MAIL ORDER' then refund_amount end) as refund_amount_mo,
       sum(case when sales_region_title = 'MAIL ORDER' then sub_total end) as gross_rev_mo,
       count(distinct case when sales_region_title = 'Tampa' then return_order_id end) as return_orders_fl,
       sum(case when sales_region_title = 'Tampa' then refund_amount end) as refund_amount_fl,
       sum(case when sales_region_title = 'Tampa' then sub_total end) as gross_rev_fl,
       count(distinct case when sales_region_title = 'Chicago' then return_order_id end) as return_orders_chi,
       sum(case when sales_region_title = 'Chicago' then refund_amount end) as refund_amount_chi,
       sum(case when sales_region_title = 'Chicago' then sub_total end) as gross_rev_chi,
       count(distinct case when sales_region_title = 'Houston' then return_order_id end) as return_orders_tx,
       sum(case when sales_region_title = 'Houston' then refund_amount end) as refund_amount_tx,
       sum(case when sales_region_title = 'Houston' then sub_total end) as gross_rev_tx,
       count(distinct case when sales_region_title = 'Seattle' then return_order_id end) as return_orders_sea,
       sum(case when sales_region_title = 'Seattle' then refund_amount end) as refund_amount_sea,
       sum(case when sales_region_title = 'Seattle' then sub_total end) as gross_rev_sea,
       count(distinct case when sales_region_title = 'New York' then return_order_id end) as return_orders_ny,
       sum(case when sales_region_title = 'New York' then refund_amount end) as refund_amount_ny,
       sum(case when sales_region_title = 'New York' then sub_total end) as gross_rev_ny,
       count(distinct case when sales_region_title = 'LA - Greater' then return_order_id end) as return_orders_la,
       sum(case when sales_region_title = 'LA - Greater' then refund_amount end) as refund_amount_la,
       sum(case when sales_region_title = 'LA - Greater' then sub_total end) as gross_rev_la,
       count(distinct case when sales_region_title = 'SF Bay Area' then return_order_id end) as return_orders_sf,
       sum(case when sales_region_title = 'SF Bay Area' then refund_amount end) as refund_amount_sf,
       sum(case when sales_region_title = 'SF Bay Area' then sub_total end) as gross_rev_sf

from   metrics.order_product op
left join (
    select order_id AS return_order_id
        , product_id
        , SUM(refund_amount) AS refund_amount
    FROM metrics.return
    WHERE (week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
          week = date_trunc('week', current_date)::date-1 - 7*53)
    GROUP BY 1,2
    ) rf ON op.order_id = rf.return_order_id AND op.product_id = rf.product_id
where  (delivery_week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week = date_trunc('week', current_date)::date-1 - 7*53)
and    order_biz_type <> 'restaurant'
group by 1,2
order by 1,2
),
base2 as (
SELECT
  delivery_week as delivery_week_b2,
  sum(discount) AS discount,
  sum(expense_pct/100.0000*revenue) AS points_expense,
  sum(net_revenue) AS net_revenue,
  sum(new_user_coupon) AS new_user_coupon,
  sum(oos) AS oos,
  --sum(revenue) AS revenue,
  sum(shipped_and_refunded) AS shipped_and_refunded,
  sum(total_discount) AS total_discount
FROM metrics.revenue_main_table_with_language
where  (delivery_week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8
or     delivery_week = date_trunc('week', current_date)::date-1 - 7*53)
group by 1),
base3 as (
select date_trunc('week', sess_date+1)::date-1 as delivery_week_b3,
       sum(orders) as orders,
       count(distinct session_id) as total_sessions
from   metrics.sessions_details
where  (delivery_week_b3 between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week_b3 = date_trunc('week', current_date)::date-1 - 7*53)
group  by 1
),
base4 as (
SELECT  date_trunc('week', response_date+1)::date-1 AS delivery_week_b4,
        round((sum(promoter_flag)-sum(detractor_flag))*100.00/count(distinct response_id),0) as nps,
/*MOF - NJ*/
        round((sum(case when response_region = 'SF Bay Area' then promoter_flag else 0 end)
              -sum(case when response_region = 'SF Bay Area' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'SF Bay Area' then response_id else null end),0) as nps_sf,

        round((sum(case when response_region = 'LA - Greater' then promoter_flag else 0 end)
              -sum(case when response_region = 'LA - Greater' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'LA - Greater' then response_id else null end),0) as nps_la,

        round((sum(case when response_region = 'Seattle' then promoter_flag else 0 end)
              -sum(case when response_region = 'Seattle' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'Seattle' then response_id else null end),0) as nps_sea,

        round((sum(case when response_region = 'New York' then promoter_flag else 0 end)
              -sum(case when response_region = 'New York' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'New York' then response_id else null end),0) as nps_ny,

        round((sum(case when response_region = 'Houston' then promoter_flag else 0 end)
              -sum(case when response_region = 'Houston' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'Houston' then response_id else null end),0) as nps_tx,

        round((sum(case when response_region = 'Chicago' then promoter_flag else 0 end)
              -sum(case when response_region = 'Chicago' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'Chicago' then response_id else null end),0) as nps_chi,

        round((sum(case when response_region = 'Tampa' then promoter_flag else 0 end)
              -sum(case when response_region = 'Tampa' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'Tampa' then response_id else null end),0) as nps_fl,

        round((sum(case when response_region = 'MAIL ORDER - WEST' then promoter_flag else 0 end)
              -sum(case when response_region = 'MAIL ORDER - WEST' then detractor_flag else 0 end))*100.00
   /count(distinct case when response_region = 'MAIL ORDER - WEST' then response_id else null end),0) as nps_mo
FROM   metrics.survey_user_response_detail
where  (delivery_week_b4 between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week_b4 = date_trunc('week', current_date)::date-1 - 7*53)
and   question_id = 1
group by 1
),
base5 as (
SELECT  date_trunc('week', delivery_day+1)::date-1 as delivery_week_b5,
        1-(sum(not_perfect_orders)*1.00/sum(total_orders)) as perfect_order_rate,
        1-(sum(case when region = '1 - SF Bay Area' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '1 - SF Bay Area' then total_orders else 0 end)) as perfect_order_rate_sf,
        1-(sum(case when region = '2 - LA - Greater' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '2 - LA - Greater' then total_orders else 0 end)) as perfect_order_rate_la,
        1-(sum(case when region = '4 - Seattle' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '4 - Seattle' then total_orders else 0 end)) as perfect_order_rate_sea,
        1-(sum(case when region = '7 - New York' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '7 - New York' then total_orders else 0 end)) as perfect_order_rate_ny,
        1-(sum(case when region = '10 - Houston' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '10 - Houston' then total_orders else 0 end)) as perfect_order_rate_tx,
        1-(sum(case when region = '15 - Chicago' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '15 - Chicago' then total_orders else 0 end)) as perfect_order_rate_chi,
        1-(sum(case when region = '23 - Tampa' then not_perfect_orders else 0 end)*1.00
          /sum(case when region = '23 - Tampa' then total_orders else 0 end)) as perfect_order_rate_fl
FROM   metrics.main_dashboard_perfect_order_rate
where  (delivery_week_b5 between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week_b5 = date_trunc('week', current_date)::date-1 - 7*53)
and    delivery_type = 'grocery'
group  by 1
),
new_booking as (
SELECT order_day as time_day_unit,order_week as time_week_unit,
       CASE WHEN device_type = 'APP iOS' THEN 'iOS'
            WHEN device_type = 'APP Android' THEN 'Android'
            ELSE 'Web' END AS Device,
       CASE WHEN language = 'zh' THEN 'Chinese'
            WHEN language = 'zh-Hant' THEN 'Chinese'
            WHEN language = 'en' THEN 'English'
            ELSE 'OTHER' END AS Language,
       CASE WHEN sales_org_id IN (3,16) THEN 'Mail'
            ELSE 'Grocery' END AS Region,
       count(distinct buyer_id) as new_d2c
FROM metrics.order
WHERE payment_mode = 'F'
      AND daily_new_users_flag_order = 1
      AND order_day between '2020-12-27' and dateadd('day',-1,current_date)::DATE
GROUP BY 1,2,3,4,5),
new_revenue as (
SELECT delivery_day as time_day_unit,delivery_week as time_week_unit,
       CASE WHEN device_type = 'APP iOS' THEN 'iOS'
            WHEN device_type = 'APP Android' THEN 'Android'
            ELSE 'Web' END AS Device,
       CASE WHEN language = 'zh' THEN 'Chinese'
            WHEN language = 'zh-Hant' THEN 'Chinese'
            WHEN language = 'en' THEN 'English'
            ELSE 'OTHER' END AS Language,
       CASE WHEN sales_org_id IN (3,16) THEN 'Mail'
            ELSE 'Grocery' END AS Region,
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
(select device,language,region,wd,sum(revenue)/sum(cohort_buyers) as avg_ltv
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
FROM
(SELECT order_day  AS time_day_unit,
        order_week as time_week_unit,
    SUM(od.amount) AS new_user_book_coupon_amount
FROM metrics.order o
LEFT JOIN weee_p01.gb_coupon_use c ON o.order_id = c.order_id
LEFT JOIN weee_p01.gb_coupon_code cc ON c.code = cc.code
 LEFT JOIN weee_p01.gb_order_discount od ON od.order_id = o.order_id
WHERE order_day >='2020-12-27'
AND payment_mode = 'F'
and o.discount > 0
AND od.type = 'coupon'
AND (o.coupon = 'sign_up_coupon' or o.coupon = 'order_share_sign_up_coupon')
GROUP BY 1,2) a

full outer join

(SELECT delivery_day  AS time_day_unit,
       delivery_week as time_week_unit,
       SUM(od.amount) AS new_user_rev_coupon_amount
FROM metrics.order o
LEFT JOIN weee_p01.gb_coupon_use c ON o.order_id = c.order_id
LEFT JOIN weee_p01.gb_coupon_code cc ON c.code = cc.code
 LEFT JOIN weee_p01.gb_order_discount od ON od.order_id = o.order_id
WHERE delivery_day >='2020-12-27'
AND payment_mode = 'F'
and o.discount > 0
AND od.type = 'coupon'
AND (o.coupon = 'sign_up_coupon' or o.coupon = 'order_share_sign_up_coupon')
GROUP BY 1,2) b
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
select     a.time_week_unit as delivery_week_b6,
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
where  (delivery_week_b6 between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week_b6 = date_trunc('week', current_date)::date-1 - 7*53)
group by 1
),
base7 as (
SELECT  date_trunc('week', delivery_day+1)::date-1 as delivery_week_b7,
        sum(case when order_type = 'RTG' then order_total else 0 end) as sc_booking,
        sum(case when order_type = 'RTG' then cm_margin else 0 end)/sum(case when order_type = 'RTG' then cm_base else 0 end) as sc_cm,
        sum(case when order_type in ('WeeeOD', 'Ricepo', 'Ricepo_vip_order') then order_total else 0 end) as od_booking,
        sum(case when order_type in ('WeeeOD', 'Ricepo', 'Ricepo_vip_order') then cm_margin else 0 end)/sum(case when order_type in ('WeeeOD', 'Ricepo') then cm_base else 0 end) as od_cm,
        sum(order_total) as sc_od_booking,
        sum(cm_margin)/sum(cm_base) as sc_od_cm
FROM   dws.restaurant_order
where  (delivery_week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week = date_trunc('week', current_date)::date-1 - 7*53)
group by 1
),
base8 as (
SELECT  delivery_week as delivery_week_b8,
        sum(case when            division = 'Produce' then sub_total else 0 end)   as produce_rev,
        count(distinct case when division = 'Produce' then buyer_id else null end) as produce_user,
        sum(case when   division = 'Produce' then gm_margin else null end)
       /sum( case when   division = 'Produce' then gm_base else null end)  as produce_gm,

        sum(case when            division = 'Protein' then sub_total else 0 end)   as protein_rev,
        count(distinct case when division = 'Protein' then buyer_id else null end) as protein_user,
        sum(case when   division = 'Protein' then gm_margin else null end)
       /sum(case when   division = 'Protein' then gm_base else null end)  as protein_gm,

        sum(case when            division = 'Frozen' then sub_total else 0 end)   as frozen_rev,
        count(distinct case when division = 'Frozen' then buyer_id else null end) as frozen_user,
        sum(case when   division = 'Frozen' then gm_margin else null end)
       /sum(case when   division = 'Frozen' then gm_base else null end)  as frozen_gm,

        sum(case when            division = 'Dairy/Deli' then sub_total else 0 end)   as dairy_rev,
        count(distinct case when division = 'Dairy/Deli' then buyer_id else null end) as dairy_user,
        sum(case when   division = 'Dairy/Deli' then gm_margin else null end)
       /sum(case when   division = 'Dairy/Deli' then gm_base else null end)  as dairy_gm,

        sum(case when            division = 'Dry Grocery' then sub_total else 0 end)   as dry_rev,
        count(distinct case when division = 'Dry Grocery' then buyer_id else null end) as dry_user,
        sum(case when   division = 'Dry Grocery' then gm_margin else null end)
       /sum(case when   division = 'Dry Grocery' then gm_base else null end)  as dry_gm,

        sum(case when            division = 'GM & HBC' then sub_total else 0 end)   as gm_rev,
        count(distinct case when division = 'GM & HBC' then buyer_id else null end) as gm_user,
        sum(case when   division = 'GM & HBC' then gm_margin else null end)
       /sum(case when   division = 'GM & HBC' then gm_base else null end)  as gm_gm
FROM   metrics.order_product
where  (delivery_week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        delivery_week = date_trunc('week', current_date)::date-1 - 7*53)
group by 1
)
, base9 AS (
  SELECT DATE(delivery_week)  as delivery_week_b9
    , gross_rev AS gross_rev_plan
    , net_rev AS net_rev_plan
    , prod_gm_pct AS prod_gm_plan
  FROM sandbox.ggs_wbr_raw_data
  WHERE DATE(delivery_week) between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
        (DATE(delivery_week) = date_trunc('week', current_date)::date-1 - 7*53)
)
, base10 AS (
    SELECT DATE(delivery_week) AS delivery_week_10
        , CASE WHEN user_ethnicity = 'South Asian' THEN 'Indian' ELSE user_ethnicity END AS user_ethnicity
        , SUM(CASE WHEN metric_nm = 'New Users' then value_cnt ELSE 0 END) AS new_user_plan
        , SUM(CASE WHEN metric_nm = 'Active Users' then value_cnt ELSE 0 END) AS active_users_plan
        , AVG(CASE WHEN metric_nm = 'ARPU' then value_cnt ELSE 0 END) AS arpu_plan_avg
        , SUM(CASE WHEN metric_nm = 'Gross Revenue' then value_cnt ELSE 0 END) AS rev_plan
     FROM sandbox.ggs_wbr_plan_ethnic_raw_data
     GROUP BY 1 ,2
)
, base11 AS (
SELECT delivery_week AS delivery_week_11,
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
    WHERE (delivery_week between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
          delivery_week = date_trunc('week', current_date)::date-1 - 7*53)
    group by 1 )
, cal_base as
    (select week_begin_date as delivery_week,
    count(distinct case when date_trunc('month',cal_date) = date_trunc('month',week_begin_date) then cal_date else null end) as same_month,
    max(case when date_trunc('month',cal_date) != date_trunc('month',week_begin_date) then 1 else 0 end) as flag
from weee_p01.calendar
where (week_begin_date between date_trunc('week', current_date)::date-1 - 7*14 and date_trunc('week', current_date)::date-8 or
          week_begin_date = date_trunc('week', current_date)::date-1 - 7*53)
group by 1)
, gm_plan_t1 as
(select cb.delivery_week,
     avg(wbr1.dairy_deli * same_month + (1-same_month) * wbr2.dairy_deli ) as dairy_deli_plan,
     avg(wbr1.protein * same_month + (1-same_month) * wbr2.protein ) as protein_plan,
    avg(wbr1.frozen * same_month + (1-same_month) * wbr2.frozen) as frozen_plan,
    avg(wbr1.produce * same_month + (1-same_month) * wbr2.produce ) as produce_plan,
       avg(wbr1.dry_grocery * same_month + (1-same_month) * wbr2.dry_grocery ) as dry_grocery_plan,
       avg(wbr1.gm_hbc * same_month + (1-same_month) * wbr2.gm_hbc) as gm_hbc_plan
from cal_base cb
left join sandbox.ggs_wbr_division_plan wbr1 on date(wbr1.delivery_month) = date(date_trunc('month',delivery_week))
    left join sandbox.ggs_wbr_division_plan wbr2 on date_add('month', 1, date(wbr2.delivery_month)) = date_trunc('month', delivery_week)
where flag = 1
group by 1)
, gm_plan_t2 as (
    select delivery_week,
       dairy_deli as dairy_deli_plan,
       protein as protein_plan,
       frozen as frozen_plan,
       produce as produce_plan,
       dry_grocery as dry_grocery_plan,
       gm_hbc as gm_hbc_plan
from cal_base cb
left join sandbox.ggs_wbr_division_plan wbr on date(wbr1.delivery_month) = date(date_trunc('month',delivery_week))
where flag =0)
, base_12 as (select * from gm_plan_t1 union all select * from gm_plan_t2 )
select *
from      base1 b1
left join base2 b2
on b1.delivery_week = b2.delivery_week_b2
and b1.ethnicity_user = 'Chinese'
left join base3 b3
on b1.delivery_week = b3.delivery_week_b3
and b1.ethnicity_user = 'Chinese'
left join base4 b4
on b1.delivery_week = b4.delivery_week_b4
and b1.ethnicity_user = 'Chinese'
left join base5 b5
on b1.delivery_week = b5.delivery_week_b5
and b1.ethnicity_user = 'Chinese'
left join base6 b6
on b1.delivery_week = b6.delivery_week_b6
and b1.ethnicity_user = 'Chinese'
left join base7 b7
on b1.delivery_week = b7.delivery_week_b7
and b1.ethnicity_user = 'Chinese'
left join base8 b8
on b1.delivery_week = b8.delivery_week_b8
and b1.ethnicity_user = 'Chinese'
left join base9 b9
ON b1.delivery_week = b9.delivery_week_b9
and b1.ethnicity_user  = 'Chinese'
left join base10 b10
on b1.delivery_week = b10.delivery_week_10
and b1.ethnicity_user = b10.user_ethnicity
left join base11 b11
on b1.delivery_week = b11.delivery_week_11
and b1.ethnicity_user = 'Chinese'
left join base12 b12 
on b1.delivery_week = b12.delivery_week_12 
and b1.ethnicity_user = 'Chinese'
;