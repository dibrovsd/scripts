drop view reports.all_police;

create or replace view reports.all_police as
with police as (
    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document2 t

    union all

    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document3 t

    union all

    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document4 t

    union all

    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document9 t

    union all

    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document11 t

    union all

    select
        t.id,
        2 as project_id,
        t.n_contract,
        t.d_issue,
        t.canceled,
        t.inscompany_id
    from docflow_document12 t
)
select
    p.*,
    inscompany.title as inscompany
from police p
join docflow_inscompany inscompany on inscompany.id = p.inscompany_id