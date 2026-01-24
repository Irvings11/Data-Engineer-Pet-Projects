---total orders
---total sales
---total items
---average order value
---sales by category
---top selling items
---orders by hour
---sales by hour
---orders by address
---orders by delivery/pick up

select
o.order_id,
i.item_price,
o.quantity,
i.item_cat,
i.item_name,
o.created_at,
a.delivery_address1,
a.delivery_address2,
a.delivery_city,
a.delivery_zipcode,
o.delivery
from "order" o
left join item i on o.item_id = i.item_id
left join address a on o.add_id = a.add_id


---total quantity by ingredient
---total cost of ingredients
---calculated cost of pizza
---percentage stock remaining by ingredient

select
o.item_id,
i.sku,
i.item_name,
r.ing_id,
ing.ing_name,
r.quantity as recipe_quantity,
sum(o.quantity) as order_quantity
from "order" o 
left join item i on o.item_id = i.item_id
left join recipe r on i.sku = r.recipe_id
left join ingredient ing on ing.ing_id = r.ing_id
group by 
o.item_id, i.sku, i.item_name,
r.ing_id, r.quantity,ing.ing_name

---
select 
s1.item_name,
s1.ing_id,
s1.ing_name,
s1.ing_price,
s1.ing_weight,
s1.recipe_quantity,
s1.order_quantity,
s1.order_quantity * s1.recipe_quantity as ordered_weight,
s1.ing_price/s1.ing_weight as unit_cost,
(s1.order_quantity*s1.recipe_quantity)*(s1.ing_price/s1.ing_weight) as ingredient_cost
from (select
o.item_id,
i.sku,
i.item_name,
r.ing_id,
ing.ing_name,
r.quantity as recipe_quantity,
sum(o.quantity) as order_quantity,
ing.ing_price,
ing.ing_weight
from "order" as o
left join item i on o.item_id = i.item_id
left join recipe r on i.sku = r.recipe_id
left join ingredient ing on ing.ing_id = r.ing_id
group by o.item_id, i.sku, i.item_name,r.ing_id, 
r.quantity, ing.ing_name, ing.ing_price,ing.ing_weight) as s1

---
select
r.date,
s.first_name,
s.last_name,
s.hourly_rate,
sh.start_time,
sh.end_time,
EXTRACT(EPOCH FROM (sh.end_time - sh.start_time)) / 3600 AS hours_in_shift,
(EXTRACT(EPOCH FROM (sh.end_time - sh.start_time)) / 3600) * s.hourly_rate AS staff_cost
from rota r
left join staff s on r.staff_id = s.staff_id
left join shift sh on r.shift_id = sh.shift_id
--

---
UPDATE stock1
SET
  total_inv_weight = ing_weight * quantity,
  remaining_weight = (ing_weight * quantity) - ordered_weight;
  SELECT * FROM stock1;

select
s2.ing_name,
s2.ordered_weight,
ing.ing_weight,
inv.quantity,
ing.ing_weight*inv.quantity as total_inv_weight,
(ing.ing_weight * inv.quantity)-s2.ordered_weight as remaining_weight
from (
select ing_id, ing_name,
sum(ordered_weight) as ordered_weight
from stock1
group by ing_name,ing_id) s2
left join inventory inv on inv.item_id = s2.ing_id
left join ingredient ing on ing.ing_id = s2.ing_id
---
SELECT
  r.date,
  s.first_name,
  s.last_name,
  s.hourly_rate,
  sh.start_time,
  sh.end_time,
  CASE
    WHEN sh.end_time >= sh.start_time THEN
      EXTRACT(EPOCH FROM (sh.end_time - sh.start_time)) / 3600
    ELSE
      EXTRACT(EPOCH FROM ((sh.end_time + INTERVAL '24 hours') - sh.start_time)) / 3600
  END AS hours_in_shift,
  CASE
    WHEN sh.end_time >= sh.start_time THEN
      (EXTRACT(EPOCH FROM (sh.end_time - sh.start_time)) / 3600) * s.hourly_rate
    ELSE
      (EXTRACT(EPOCH FROM ((sh.end_time + INTERVAL '24 hours') - sh.start_time)) / 3600) * s.hourly_rate
  END AS staff_cost
FROM rota r
LEFT JOIN staff s ON r.staff_id = s.staff_id
LEFT JOIN shift sh ON r.shift_id = sh.shift_id;