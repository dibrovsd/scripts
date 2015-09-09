create or replace view v_baza as 
select * 
from baza t
where (
    sened like '050%' or
    sened like '50%' or
    sened like '055%' or
    sened like '55%' or
    sened like '051%' or
    sened like '51%' or
    sened like '070%' or
    sened like '70%' or
    sened like '071%' or
    sened like '71%' or
    sened like '% 050%' or
    sened like '% 50%' or
    sened like '% 055%' or
    sened like '% 55%' or
    sened like '% 051%' or
    sened like '% 51%' or
    sened like '% 070%' or
    sened like '% 70%' or
    sened like '% 071%' or
    sened like '% 71%' or
    sened like '%99450%' or
    sened like '%+99450%' or
    sened like '%99455%' or
    sened like '%+99455%' or
    sened like '%99451%' or
    sened like '%+99451%' or
    sened like '%99470%' or
    sened like '%+99470%' or
    sened like '%99471%' or
    sened like '%+99471%' or
    sened like '%99477%' or
    sened like '%+99477%' or 

    sened like '% т050%' or
    sened like '% т50%' or
    sened like '% т055%' or
    sened like '% т55%' or
    sened like '% т051%' or
    sened like '% т51%' or
    sened like '% т070%' or
    sened like '% т70%' or
    sened like '% т071%' or
    sened like '% т71%' or
    
    sened like '%т050%' or
    sened like '%т50%' or
    sened like '%т055%' or
    sened like '%т55%' or
    sened like '%т051%' or
    sened like '%т51%' or
    sened like '%т070%' or
    sened like '%т70%' or
    sened like '%т071%' or
    sened like '%т71%' or

    sened like '%т99450%' or
    sened like '%т+99450%' or
    sened like '%т99455%' or
    sened like '%т+99455%' or
    sened like '%т99451%' or
    sened like '%т+99451%' or
    sened like '%т99470%' or
    sened like '%т+99470%' or
    sened like '%т99471%' or
    sened like '%т+99471%' or
    sened like '%т99477%' or
    sened like '%т+99477%' or 

    sened like '% т.050%' or
    sened like '% т.50%' or
    sened like '% т.055%' or
    sened like '% т.55%' or
    sened like '% т.051%' or
    sened like '% т.51%' or
    sened like '% т.070%' or
    sened like '% т.70%' or
    sened like '% т.071%' or
    sened like '% т.71%' or

    sened like '%т.050%' or
    sened like '%т.50%' or
    sened like '%т.055%' or
    sened like '%т.55%' or
    sened like '%т.051%' or
    sened like '%т.51%' or
    sened like '%т.070%' or
    sened like '%т.70%' or
    sened like '%т.071%' or
    sened like '%т.71%' or

    sened like '%т.99450%' or
    sened like '%т.+99450%' or
    sened like '%т.99455%' or
    sened like '%т.+99455%' or
    sened like '%т.99451%' or
    sened like '%т.+99451%' or
    sened like '%т.99470%' or
    sened like '%т.+99470%' or
    sened like '%т.99471%' or
    sened like '%т.+99471%' or
    sened like '%т.99477%' or
    sened like '%т.+99477%' or 

    sened like '% тел.050%' or
    sened like '% тел.50%' or
    sened like '% тел.055%' or
    sened like '% тел.55%' or
    sened like '% тел.051%' or
    sened like '% тел.51%' or
    sened like '% тел.070%' or
    sened like '% тел.70%' or
    sened like '% тел.071%' or
    sened like '% тел.71%' or

    sened like '%тел.050%' or
    sened like '%тел.50%' or
    sened like '%тел.055%' or
    sened like '%тел.55%' or
    sened like '%тел.051%' or
    sened like '%тел.51%' or
    sened like '%тел.070%' or
    sened like '%тел.70%' or
    sened like '%тел.071%' or
    sened like '%тел.71%' or

    sened like '%тел.99450%' or
    sened like '%тел.+99450%' or
    sened like '%тел.99455%' or
    sened like '%тел.+99455%' or
    sened like '%тел.99451%' or
    sened like '%тел.+99451%' or
    sened like '%тел.99470%' or
    sened like '%тел.+99470%' or
    sened like '%тел.99471%' or
    sened like '%тел.+99471%' or
    sened like '%тел.99477%' or
    sened like '%тел.+99477%' or 
    
    sened like '% тел050%' or
    sened like '% тел50%' or
    sened like '% тел055%' or
    sened like '% тел55%' or
    sened like '% тел051%' or
    sened like '% тел51%' or
    sened like '% тел070%' or
    sened like '% тел70%' or
    sened like '% тел071%' or
    sened like '% тел71%' or

    sened like '%тел050%' or
    sened like '%тел50%' or
    sened like '%тел055%' or
    sened like '%тел55%' or
    sened like '%тел051%' or
    sened like '%тел51%' or
    sened like '%тел070%' or
    sened like '%тел70%' or
    sened like '%тел071%' or
    sened like '%тел71%' or

    sened like '%тел99450%' or
    sened like '%тел+99450%' or
    sened like '%тел99455%' or
    sened like '%тел+99455%' or
    sened like '%тел99451%' or
    sened like '%тел+99451%' or
    sened like '%тел99470%' or
    sened like '%тел+99470%' or
    sened like '%тел99471%' or
    sened like '%тел+99471%' or
    sened like '%тел99477%' or
    sened like '%тел+99477%'

)
and (nomznak like '10%' or nomznak like '90%' or nomznak like '99%')