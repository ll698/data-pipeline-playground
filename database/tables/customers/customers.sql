CREATE TABLE IF NOT EXISTS customers (
    customer_id CHAR(256) NOT NULL,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(15),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24),
    delivery_key CHAR(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_key ON customers (delivery_key);

CREATE TABLE IF NOT EXISTS customers_audit (
    audit_ts timestamptz not null default now(),
    operation varchar(10)not null,
    username text not null default "current_user"(),
    before jsonb,
    after jsonb
);

CREATE OR REPLACE FUNCTION customers_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin

IF TG_OP = 'INSERT'
THEN
INSERT INTO customers_audit (operation, after)
VALUES (TG_OP, to_jsonb(NEW));
RETURN NEW;

ELSIF TG_OP = 'UPDATE'
THEN
IF NEW != OLD THEN
 INSERT INTO customers_audit (operation, before, after)
VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));
END IF;
 RETURN NEW;

ELSIF TG_OP = 'DELETE'
THEN
INSERT INTO customers_audit (operation, before)
VALUES (TG_OP, to_jsonb(OLD));
RETURN OLD;
END IF;
end;
$function$ ;

CREATE TRIGGER customers_audit_trig
 BEFORE INSERT OR UPDATE OR DELETE
 ON customers
 FOR EACH ROW
 EXECUTE PROCEDURE customers_audit_trig();