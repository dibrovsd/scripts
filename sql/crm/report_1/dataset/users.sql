SELECT id,
       last_name ||' '|| first_name AS title
FROM base_user
WHERE crm_role = 1
