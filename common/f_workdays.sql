create function f_workdays(date, date) returns integer
as $$
    with params as (
        select $1 as d_start,
            $2 as d_end
    ),

    dates as (
    	select generate_series(params.d_start, params.d_end, interval '1 day')::date as day
    	from params
    )

    select count(1)::integer from dates
    where date_part('isodow', dates.day) between 1 and 5
$$
language sql
STABLE
returns null on null input;
