-- Source and target tables are identical, but not in the same column order
-- target is initially empty

SET search_path TO public;
CREATE TABLE gebieden_ggwgebieden_new (
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_stadsdeel_id character varying(255),
    ligt_in_stadsdeel_identificatie character varying(255),
    ligt_in_stadsdeel_volgnummer integer,
    id character varying(255)
);
CREATE TABLE gebieden_ggwgebieden (
    id character varying(255),
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_stadsdeel_id character varying(255),
    ligt_in_stadsdeel_identificatie character varying(255),
    ligt_in_stadsdeel_volgnummer integer
);
INSERT INTO gebieden_ggwgebieden_new (identificatie, volgnummer, ligt_in_stadsdeel_id,
    ligt_in_stadsdeel_identificatie, ligt_in_stadsdeel_volgnummer) VALUES ('01', 1, '01.1', '01', 1);
