-- The main table and also the related table

SET search_path TO public;
CREATE TABLE gebieden_ggwgebieden_new (
    id character varying(255),
    identificatie character varying(255),
    volgnummer integer,
    ligt_in_stadsdeel_id character varying(255),
    ligt_in_stadsdeel_identificatie character varying(255),
    ligt_in_stadsdeel_volgnummer integer
);
CREATE TABLE gebieden_ggwgebieden_new_bestaat_uit_buurten (
    id character varying(255),
    ggwgebieden_id character varying(255),
    bestaat_uit_buurten_id character varying(255),
    ggwgebieden_identificatie character varying(255),
    ggwgebieden_volgnummer integer,
    bestaat_uit_buurten_identificatie character varying(255),
    bestaat_uit_buurten_volgnummer integer
);


INSERT INTO gebieden_ggwgebieden_new (identificatie, volgnummer, ligt_in_stadsdeel_id,
    ligt_in_stadsdeel_identificatie, ligt_in_stadsdeel_volgnummer) VALUES ('01', 1, '01.1', '01', 1);

INSERT INTO gebieden_ggwgebieden_new_bestaat_uit_buurten (id, ggwgebieden_id, bestaat_uit_buurten_id,
    ggwgebieden_identificatie, ggwgebieden_volgnummer, bestaat_uit_buurten_identificatie,
    bestaat_uit_buurten_volgnummer) VALUES ('01.1.01.1', '01.1', '01.1', '01', 1, '01', 1);
