-- Source and target tables

SET search_path TO public;
CREATE TABLE gebieden_ggwgebieden_new (
    id character varying(255),
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_stadsdeel_id character varying(255),
    ligt_in_stadsdeel_identificatie character varying(255),
    ligt_in_stadsdeel_volgnummer integer
);

CREATE SCHEMA gebieden;

CREATE TABLE gebieden.ggwgebieden (
    id character varying(255),
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_stadsdeel_id character varying(255),
    ligt_in_stadsdeel_identificatie character varying(255),
    ligt_in_stadsdeel_volgnummer integer
);

CREATE VIEW gebieden_ggwgebieden AS SELECT * FROM gebieden.ggwgebieden;

INSERT INTO gebieden_ggwgebieden_new (identificatie, volgnummer, ligt_in_stadsdeel_id,
    ligt_in_stadsdeel_identificatie, ligt_in_stadsdeel_volgnummer) VALUES
        ('01', 1, '01.1', '01', 1),
        ('02', 1, '02.1', '01', 1);

INSERT INTO gebieden_ggwgebieden (identificatie, volgnummer, ligt_in_stadsdeel_id,
    ligt_in_stadsdeel_identificatie, ligt_in_stadsdeel_volgnummer) VALUES ('01', 1, '01.1', '01', 1);
