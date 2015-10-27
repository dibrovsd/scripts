{{datasets.src.sql}},

base_gr as (
    select
        op.action,
        op.region,
        op.glass_type,
        --
        count(case when op.m = 'send_to_ins' then days_repair end) as days_repair_cnt,
        sum(case when op.m = 'send_to_ins' then days_repair end) as days_repair_sum,
        --
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 4 then 1 end) as days_repair_cnt_0_4,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 9 then 1 end) as days_repair_cnt_0_9,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 14 then 1 end) as days_repair_cnt_0_14,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 24 then 1 end) as days_repair_cnt_0_24,
        count(case when op.m = 'send_to_ins' and days_repair >= 24 then 1 end) as days_repair_cnt_25,
        --
        count(case when op.m = 'pay' then days_documents end) as days_documents_cnt,
        sum(case when op.m = 'pay' then days_documents end) as days_documents_sum,
        --
        count(case when op.m = 'pay' then days_payment end) as days_payment_cnt,
        sum(case when op.m = 'pay' then days_payment end) as days_payment_sum,
        --
        count(case when op.m = 'pay' then days_summary end) as days_summary_cnt,
        sum(case when op.m = 'pay' then days_summary end) as days_summary_sum
    from operations op
    group by op.action, op.region, op.glass_type
)


select * from base_gr
