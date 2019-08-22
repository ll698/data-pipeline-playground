
CREATE TABLE IF NOT EXISTS products (
    product_id SMALLINT NOT NULL,
    product_name VARCHAR(40),
    supplier_id SMALLINT,
    category_id SMALLINT,
    quantity_per_unit VARCHAR(20),
    unit_price REAL,
    units_in_stock SMALLINT,
    units_on_order SMALLINT,
    reorder_level SMALLINT,
    discontinued INTEGER NOT NULL,
    delivery_key CHAR(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_key ON products (delivery_key);

CREATE TABLE IF NOT EXISTS products_audit (
    audit_ts timestamptz not null default now(),
    operation varchar(10)not null,
    username text not null default "current_user"(),
    before jsonb,
    after jsonb
);

CREATE OR REPLACE FUNCTION products_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin

IF TG_OP = 'INSERT'
THEN
INSERT INTO products_audit (operation, after)
VALUES (TG_OP, to_jsonb(NEW));
RETURN NEW;

ELSIF TG_OP = 'UPDATE'
THEN
IF NEW != OLD THEN
 INSERT INTO products_audit (operation, before, after)
VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));
END IF;
 RETURN NEW;

ELSIF TG_OP = 'DELETE'
THEN
INSERT INTO products_audit (operation, before)
VALUES (TG_OP, to_jsonb(OLD));
RETURN OLD;
END IF;
end;
$function$ ;

CREATE TRIGGER products_audit_trig
 BEFORE INSERT OR UPDATE OR DELETE
 ON products
 FOR EACH ROW
 EXECUTE PROCEDURE products_audit_trig();