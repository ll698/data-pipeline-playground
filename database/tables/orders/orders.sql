CREATE TABLE IF NOT EXISTS orders (
 order_id SMALLINT NOT NULL,
 customer_id CHAR(256),
 employee_id SMALLINT,
 order_date DATE,
 required_date DATE,
 shipped_date DATE,
 ship_via SMALLINT,
 freight REAL,
 ship_name VARCHAR(40),
 ship_address VARCHAR(60),
 ship_city VARCHAR(15),
 ship_region VARCHAR(15),
 ship_postal_code VARCHAR(10),
 ship_country VARCHAR(15),
 delivery_key CHAR(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_key ON orders (delivery_key);

CREATE TABLE IF NOT EXISTS orders_audit (
    audit_ts timestamptz not null default now(),
    operation varchar(10)not null,
    username text not null default "current_user"(),
    before jsonb,
    after jsonb
);

CREATE OR REPLACE FUNCTION orders_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin

IF TG_OP = 'INSERT'
THEN
INSERT INTO orders_audit (operation, after)
VALUES (TG_OP, to_jsonb(NEW));
RETURN NEW;

ELSIF TG_OP = 'UPDATE'
THEN
IF NEW != OLD THEN
 INSERT INTO orders_audit (operation, before, after)
VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));
END IF;
 RETURN NEW;

ELSIF TG_OP = 'DELETE'
THEN
INSERT INTO orders_audit (operation, before)
VALUES (TG_OP, to_jsonb(OLD));
RETURN OLD;
END IF;
end;
$function$ ;

CREATE TRIGGER orders_audit_trig
 BEFORE INSERT OR UPDATE
 ON orders
 FOR EACH ROW
 EXECUTE PROCEDURE orders_audit_trig();