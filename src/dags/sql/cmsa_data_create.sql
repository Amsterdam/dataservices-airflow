/* create tables, if not exists */
CREATE TABLE IF NOT EXISTS cmsa_sensor_new (
    id varchar(64) PRIMARY KEY NOT NULL,
    referentiecode character varying(128),
    naam character varying(128),
    omschrijving text,
    "type" varchar(32),
    doel text
);

CREATE TABLE  IF NOT EXISTS cmsa_locatie_new (
    id SERIAL PRIMARY KEY,
    sensor_id varchar(64) REFERENCES cmsa_sensor_new(id),
    referentiecode varchar(128),
    naam varchar(128),
    "geometry" geometry(Geometry,28992)
);

CREATE TABLE IF NOT EXISTS cmsa_markering_new (
    id integer GENERATED ALWAYS AS IDENTITY,
    sensor_id varchar(64) REFERENCES cmsa_sensor_new(id),
    locatie_id integer REFERENCES cmsa_locatie_new(id),
    sensornaam varchar(128),
    sensortype varchar(32),
    "geometry" geometry(Geometry,28992)
);

/* 
Empty existing data if present
The cascade because of the foreign key references
Alternatively, before a truncate the fk contraint can be dropped and after re-created, or
one can use a DELETE instead of truncate
*/
TRUNCATE TABLE cmsa_locatie_new CASCADE;
TRUNCATE TABLE cmsa_sensor_new CASCADE;

