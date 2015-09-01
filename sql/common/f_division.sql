create function f_division(float, float) returns float
as 'select case when $2 > 0 then $1 / $2 end;'
language sql
STABLE
returns null on null input;
