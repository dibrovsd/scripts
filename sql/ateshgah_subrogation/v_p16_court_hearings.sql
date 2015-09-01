create or replace view reports.v_p16_court_hearings as 
with t as (
    select
        t.id,
        unnest(xpath ('/xml/item'::text, t.court_hearings::xml)) as court_hearings
    from docflow_document16 t
    where t.court_hearings is not null
),
t1 as (
    select
        t.id,
        (xpath('/item/@d_get', court_hearings))[1]::varchar as d_get,
        (xpath('/item/@num', court_hearings))[1]::varchar as num,
        (xpath('/item/@hearing_type', court_hearings))[1]::varchar as hearing_type_id,
        (xpath('/item/@d_plan', court_hearings))[1]::varchar as d_plan,
        (xpath('/item/@d_fact', court_hearings))[1]::varchar as d_fact,
        (xpath('/item/@court', court_hearings))[1]::varchar as court_id,
        (xpath('/item/@result', court_hearings))[1]::varchar as result_id
    from t
)
select 
    id,
    case 
        when d_get != '' then to_date(d_get, 'dd.mm.yyyy')
        else null 
    end as d_get,
    num,
    case when hearing_type_id != '' then hearing_type_id::int 
        else null
    end as hearing_type_id,
    case 
        when d_plan != '' then to_timestamp(d_plan, 'yyyy-mm-dd hh24:mi:ss')
        else null 
    end as d_plan,
    case 
        when d_fact != '' then to_timestamp(d_fact, 'yyyy-mm-dd hh24:mi:ss')
        else null 
    end as d_fact,
    case when court_id != '' then court_id::int 
        else null
    end as court_id,
    case when result_id != '' then result_id::int 
        else null
    end as result_id
from t1