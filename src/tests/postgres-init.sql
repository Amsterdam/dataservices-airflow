SET search_path TO public;
CREATE TABLE gebieden_buurten_new (
    id character varying(255),
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_wijk_id character varying(255),
    ligt_in_wijk_identificatie character varying(255),
    ligt_in_wijk_volgnummer integer
);
INSERT INTO gebieden_buurten_new (identificatie, volgnummer, ligt_in_wijk_id,
    ligt_in_wijk_identificatie, ligt_in_wijk_volgnummer) VALUES ('01', 1, '01.1', '01', 1);
