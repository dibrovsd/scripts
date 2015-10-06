SELECT id,
       last_name ||' '|| first_name AS title
FROM base_user
WHERE exists (
    select null from base_user_roles ur
    where ur.user_id = base_user.id
    and ur.role_id in ('operator', 'curator')
)
