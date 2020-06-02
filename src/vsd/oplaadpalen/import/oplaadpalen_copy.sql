BEGIN;
ALTER SEQUENCE oplaadpalen_new_id_seq OWNED BY oplaadpalen.id;
DROP TABLE IF EXISTS oplaadpalen_new;
CREATE TABLE oplaadpalen_new ( LIKE oplaadpalen INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );
INSERT INTO oplaadpalen_new SELECT * FROM oplaadpalen;
ALTER SEQUENCE oplaadpalen_new_id_seq OWNED BY oplaadpalen_new.id;

DO $$
DECLARE
  e boolean;
BEGIN
  SELECT EXISTS(
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name='oplaadpalen_new' and column_name='charging_cap_max'
    ) INTO e;
  IF NOT e THEN
    RAISE NOTICE 'Create column charging_cap_max';
    ALTER TABLE oplaadpalen_new ADD COLUMN charging_cap_max real;

    UPDATE oplaadpalen_new SET charging_cap_max = x.charging_cap_max
    FROM (
      SELECT id, MAX(charging_capability) AS charging_cap_max FROM
      (
        SELECT id, UNNEST(string_to_array(charging_capability, ';')::float[]) as charging_capability
        FROM oplaadpalen_new
      ) a
      GROUP BY id) x
    WHERE oplaadpalen_new.id = x.id;
  ELSE
    RAISE NOTICE 'Column charging_cap_max exists';
  END IF;
END $$;



COMMIT;