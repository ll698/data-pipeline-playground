CREATE TABLE IF NOT EXISTS order_details (
    order_id SMALLINT NOT NULL,
    product_id SMALLINT NOT NULL,
    unit_price REAL NOT NULL,
    quantity SMALLINT NOT NULL,
    discount REAL NOT NULL,
    delivery_key CHAR(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_key ON order_details (delivery_key);

CREATE TABLE IF NOT EXISTS order_details_audit (
    audit_ts timestamptz not null default now(),
    operation varchar(10)not null,
    username text not null default "current_user"(),
    before jsonb,
    after jsonb
);

CREATE OR REPLACE FUNCTION order_details_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin

IF TG_OP = 'INSERT'
THEN
INSERT INTO order_details_audit (operation, after)
VALUES (TG_OP, to_jsonb(NEW));
RETURN NEW;

ELSIF TG_OP = 'UPDATE'
THEN
IF NEW != OLD THEN
 INSERT INTO order_details_audit (operation, before, after)
VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));
END IF;
 RETURN NEW;

ELSIF TG_OP = 'DELETE'
THEN
INSERT INTO order_details_audit (operation, before)
VALUES (TG_OP, to_jsonb(OLD));
RETURN OLD;
END IF;
end;
$function$ ;

CREATE TRIGGER order_details_audit_trig
 BEFORE INSERT OR UPDATE OR DELETE
 ON order_details
 FOR EACH ROW
 EXECUTE PROCEDURE order_details_audit_trig();