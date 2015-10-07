create or replace view reports.v_report_17 as
WITH t AS (
    SELECT 
        t.id, 
        t.n_subrogation_claim AS "номер_дела", 
        t.name_resp AS "причинитель_ущерба", 
        t.auto_mark_resp AS "тс_причинителя_ущерба", 
        t.auto_number_resp AS "номер_тс_причинителя_ущерба", 
        (xpath('/xml/item[@is_primary="True"]/@n_phone'::text, (t.phone_resp)::xml))[1] AS "телефон_ответчика", 
        --unnest(xpath('/xml/item'::text, t.court_hearings::xml)) AS court_hearings, 
        t1.d_plan as "дата_заседания", 
        t1.num as "номер_повестки",
        t1.d_get as "повестка_получена",
        t1.hearing_type_id,
        t1.court_id,
        --
        t.type_claim_id, 
        t.s_subrogation AS "суброгационная_сумма" 
    FROM docflow_document16 t 
    join reports.v_p16_court_hearings t1 on t1.id = t.id
    WHERE t.court_hearings IS NOT NULL
), 
t4 AS (
    SELECT t.id, 
        t."дата_заседания", 
        t."номер_дела", 
        t."причинитель_ущерба", 
        t."номер_тс_причинителя_ущерба", 
        t."телефон_ответчика", 
        t2.title AS "тип_претензии", 
        t3.title AS "слушание_суд", 
        t."номер_повестки", 
        t1.title AS "тип_слушания", 
        t."повестка_получена",
        CASE WHEN t3.is_region THEN 'background-color:#f5f5f5' ELSE NULL END AS row_style
    FROM t 
    LEFT JOIN lib_courthearingtype t1 ON t1.id = t.hearing_type_id
    LEFT JOIN docflow_libtable16_71 t2 ON t2.id = t.type_claim_id
    LEFT JOIN lib_court t3 ON t3.id = t1.court_id
), 
t5 AS (
    SELECT t4.id, 
        CASE WHEN ((t4."повестка_получена")::text <> ''::text) THEN to_timestamp((t4."дата_заседания")::text, 'yyyy-mm-dd hh24:mi:ss'::text) ELSE NULL::timestamp with time zone END AS "дата_заседания", 
        t4."номер_дела", 
        t4."причинитель_ущерба", 
        t4."номер_тс_причинителя_ущерба", 
        t4."телефон_ответчика", 
        t4."тип_претензии", 
        t4."слушание_суд", 
        t4."номер_повестки", 
        t4."тип_слушания", 
        t4.row_style,
        CASE WHEN ((t4."повестка_получена")::text <> ''::text) THEN to_date((t4."повестка_получена")::text, 'dd.mm.yyyy'::text) ELSE NULL::date END AS "повестка_получена" 
    FROM t4
) 
SELECT 
    t5.id, 
    t5."дата_заседания", 
    t5."номер_дела", 
    t5."причинитель_ущерба", 
    t5."номер_тс_причинителя_ущерба", 
    t5."телефон_ответчика", 
    t5."тип_претензии", 
    t5."слушание_суд", 
    t5."номер_повестки", 
    t5."тип_слушания", 
    t5."повестка_получена",
    t5.row_style
FROM t5