/* Empty existing data if present */
TRUNCATE TABLE cmsa_markering_new;

/* Fill table with records */
INSERT INTO cmsa_markering_new (sensor_id, locatie_id, sensornaam, sensortype, geometry)
(   SELECT
    sensor.id, 
    locatie.id, 
    sensor.naam, 
    sensor.type,
    locatie.geometry
FROM cmsa_sensor_new as sensor
INNER JOIN cmsa_locatie_new as locatie
ON locatie.sensor_id = sensor.id
);
COMMIT;
