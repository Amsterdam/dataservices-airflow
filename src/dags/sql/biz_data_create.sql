BEGIN;
DROP VIEW IF EXISTS biz_view_new;
DROP TABLE IF EXISTS biz_data_new;

CREATE TABLE biz_data_new (
    biz_id integer PRIMARY KEY NOT NULL,
    naam character varying(128),
    biz_type character varying(64),
    heffingsgrondslag  character varying(128),
    website  character varying(128),
    heffing integer,
    bijdrageplichtigen integer,
    verordening  character varying(128),
    wkb_geometry  geometry(Geometry,28992)
);

ALTER TABLE biz_data_new ADD CONSTRAINT naam_unique_new UNIQUE (naam);
CREATE INDEX biz_data_new_wkb_geometry_geom_idx ON biz_data_new USING gist (wkb_geometry);

CREATE VIEW biz_view_new AS SELECT
    biz_id as id,
    naam,
    biz_type,
    heffingsgrondslag ,
    website,
    heffing,
    'EUR' as heffing_valuta_code,
    CASE heffing IS NULL
    WHEN True THEN
        NULL
    ELSE
        concat(E'\u20AC', ' ', cast(heffing as character varying(10)))
    END as heffing_display,
    bijdrageplichtigen,
    verordening ,
    wkb_geometry as geometrie
FROM biz_data_new;
COMMIT;
