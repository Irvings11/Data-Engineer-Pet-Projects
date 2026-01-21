CREATE TABLE "order" (
    "row_id" int   NOT NULL,
    "order_id" varchar(10)   NOT NULL,
    "created_at" TIMESTAMP    NOT NULL,
    "item_id" varchar(10)   NOT NULL,
    "quantity" int   NOT NULL,
    "cust_id" int   NOT NULL,
    "delivery" boolean   NOT NULL,
    "add_id" int   NOT NULL,
    CONSTRAINT "pk_order" PRIMARY KEY ("row_id")
);

CREATE TABLE "customers" (
    "cust_id" int   NOT NULL,
    "cust_firstname" varchar(50)   NOT NULL,
    "cust_lastname" varchar(50)   NOT NULL,
    CONSTRAINT "pk_customers" PRIMARY KEY (
        "cust_id"
     )
);
CREATE TABLE "address" (
    "add_id" int   NOT NULL,
    "delivery_address1" varchar(200)   NOT NULL,
    "delivery_address2" varchar(200)   NULL,
    "delivery_city" varchar(50)   NOT NULL,
    "delivery_zipcode" varchar(20)   NOT NULL,
    CONSTRAINT "pk_address" PRIMARY KEY (
        "add_id"
     )
);

CREATE TABLE "item" (
    "item_id" varchar(10)   NOT NULL,
    "sku" varchar(20)   NOT NULL,
    "item_name" varchar(100)   NOT NULL,
    "item_cat" varchar(100)   NOT NULL,
    "item_size" varchar(10)   NOT NULL,
    "item_price" decimal(10,2)   NOT NULL,
    CONSTRAINT "pk_item" PRIMARY KEY (
        "item_id"
     )
);

CREATE TABLE "ingredient" (
    "ing_id" varchar(10)   NOT NULL,
    "ing_name" varchar(200)   NOT NULL,
    "ing_weight" int   NOT NULL,
    "ing_meas" varchar(20)   NOT NULL,
    "ing_price" decimal(5,2)   NOT NULL,
    CONSTRAINT "pk_ingredient" PRIMARY KEY (
        "ing_id"
     )
);
drop table recipe
CREATE TABLE "recipe" (
    "row_id" int   NOT NULL,
    "recipe_id" varchar(20)   NOT NULL,
    "ing_id" varchar(10)   NOT NULL,
    "quantity" int   NOT NULL,
    CONSTRAINT "pk_recipe" PRIMARY KEY (
        "row_id"
     )
);
CREATE TABLE "inventory" (
    "inv_id" int   NOT NULL,
    "item_id" varchar(10)   NOT NULL,
    "quantity" int   NOT NULL,
    CONSTRAINT "pk_inventory" PRIMARY KEY (
        "inv_id"
     )
);

CREATE TABLE "staff" (
    "staff_id" varchar(20)   NOT NULL,
    "first_name" varchar(20)   NOT NULL,
    "last_name" varchar(20)   NOT NULL,
    "position" varchar(100)   NOT NULL,
    "hourly_rate" decimal(5,2)   NOT NULL,
    CONSTRAINT "pk_staff" PRIMARY KEY (
        "staff_id"
     )
);

CREATE TABLE "rota" (
    "row_id" int   NOT NULL,
    "rota_id" varchar(20)   NOT NULL,
    "date" timestamp   NOT NULL,
    "shift_id" varchar(20)   NOT NULL,
    "staff_id" varchar(20)   NOT NULL,
    CONSTRAINT "pk_rota" PRIMARY KEY (
        "row_id"
     )
);

CREATE TABLE "shift" (
    "shift_id" varchar(20)   NOT NULL,
    "day_of_week" varchar(10)   NOT NULL,
    "start_time" time   NOT NULL,
    "end_time" time   NOT NULL,
    CONSTRAINT "pk_shift" PRIMARY KEY (
        "shift_id"
     )
);
ALTER TABLE "item" ADD CONSTRAINT "uq_item_sku" UNIQUE ("sku");

-- Заказ связан с клиентом
ALTER TABLE "order" ADD CONSTRAINT "fk_order_cust_id"
FOREIGN KEY ("cust_id") REFERENCES "customers" ("cust_id");

-- Заказ связан с адресом
ALTER TABLE "order" ADD CONSTRAINT "fk_order_add_id"
FOREIGN KEY ("add_id") REFERENCES "address" ("add_id");

-- Заказ связан с товаром
ALTER TABLE "order" ADD CONSTRAINT "fk_order_item_id"
FOREIGN KEY ("item_id") REFERENCES "item" ("item_id");

-- Рецепт связан с ингредиентом
ALTER TABLE "recipe" ADD CONSTRAINT "fk_recipe_ing_id"
FOREIGN KEY ("ing_id") REFERENCES "ingredient" ("ing_id");

-- Рецепт ссылается на item через SKU
ALTER TABLE "recipe" ADD CONSTRAINT "fk_recipe_item_sku"
FOREIGN KEY ("recipe_id") REFERENCES "item" ("sku");

-- ❗️inventory связан, скорее всего, с item, а не с recipe.ing_id
-- Ниже — корректная логическая связь:
ALTER TABLE "inventory" ADD CONSTRAINT "fk_inventory_item_id"
FOREIGN KEY ("item_id") REFERENCES "item" ("item_id");

-- rota связан с сотрудником
ALTER TABLE "rota" ADD CONSTRAINT "fk_rota_staff_id"
FOREIGN KEY ("staff_id") REFERENCES "staff" ("staff_id");

-- rota связан с рабочей сменой
ALTER TABLE "rota" ADD CONSTRAINT "fk_rota_shift_id"
FOREIGN KEY ("shift_id") REFERENCES "shift" ("shift_id");

TRUNCATE TABLE "order" RESTART IDENTITY CASCADE;


SELECT * FROM item;
select * from "order";
select * from recipe;
select * from inventory;
select * from rota
select * from staff
select* from shift 
select * from customers
select * from address
select * from ingredient
