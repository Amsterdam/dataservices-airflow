ALTER TABLE precariobelasting_bedrijfsvaartuigen ADD COLUMN IF NOT EXISTS gebied VARCHAR(10);
ALTER TABLE precariobelasting_woonschepen ADD COLUMN IF NOT EXISTS  gebied VARCHAR(10);


/* BEDRIJFSVAARTUIGEN: add gebied column */
WITH precariobelasting_bedrijfsvaartuigen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number 
    the regexp is used to focus on the rate numbers only, not clutered by additional karakters like euro sign or , or -
*/
CASE 
WHEN dense_rank() over (order by regexp_replace(tarief_per_jaar_per_m2, '[^[:digit:]]', '', 'g')) = 1 THEN 'Gebied A'
ELSE 'Gebied B'
END
as gebied_type
from precariobelasting_bedrijfsvaartuigen
)
UPDATE precariobelasting_bedrijfsvaartuigen
SET gebied = gebied_type
FROM precariobelasting_bedrijfsvaartuigen_gebied
WHERE ID = identifier;
COMMIT;

/* WOONSCHEPEN: add gebied column  */
WITH precariobelasting_woonschepen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number
    the regexp is used to focus on the rate numbers only, not clutered by additional karakters like euro sign or , or -
 */
CASE 
WHEN dense_rank() over (order by regexp_replace(tarief_per_jaar_per_m2, '[^[:digit:]]', '', 'g')) = 1 THEN 'Gebied A'
ELSE 'Gebied B'
END gebied_type
from precariobelasting_woonschepen
)
UPDATE precariobelasting_woonschepen
SET gebied = gebied_type
FROM precariobelasting_woonschepen_gebied
WHERE ID = identifier;
COMMIT;


/* ------------------------------------------------------------------------------------------------ */
/* TIJDELIJK. Nodig voor geosearch. TO DO: creatie DB structuur volledig baseren op metadataschema */
/* ------------------------------------------------------------------------------------------------ */
ALTER TABLE precariobelasting_bedrijfsvaartuigen ADD COLUMN IF NOT EXISTS title VARCHAR(100);
ALTER TABLE precariobelasting_woonschepen ADD COLUMN IF NOT EXISTS  title VARCHAR(100);
ALTER TABLE precariobelasting_passagiersvaartuigen ADD COLUMN IF NOT EXISTS title VARCHAR(100);
ALTER TABLE precariobelasting_terrassen ADD COLUMN IF NOT EXISTS  title VARCHAR(100);

/* WOONSCHEPEN: add title column  */
WITH precariobelasting_woonschepen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number */
'Precariobelasting woonschepen per belastinggebied, per jaar en per m2' as title_text
from precariobelasting_woonschepen
)
UPDATE precariobelasting_woonschepen
SET title = title_text
FROM precariobelasting_woonschepen_gebied
WHERE ID = identifier;
COMMIT;

/* BEDRIJFSVAARTUIGEN: add title column  */
WITH precariobelasting_bedrijfsvaartuigen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number */
'Precariobelasting bedrijfsvaartuigen per belastinggebied, per jaar en per m2' as title_text
from precariobelasting_bedrijfsvaartuigen
)
UPDATE precariobelasting_bedrijfsvaartuigen
SET title = title_text
FROM precariobelasting_bedrijfsvaartuigen_gebied
WHERE ID = identifier;
COMMIT;

/* PASSAGIERSVAARTUIGEN: add title column  */
WITH precariobelasting_passagiersvaartuigen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number */
'Precariobelasting passagiersvaartuigen per belastinggebied, per jaar en per m2' as title_text
from precariobelasting_passagiersvaartuigen
)
UPDATE precariobelasting_passagiersvaartuigen
SET title = title_text
FROM precariobelasting_passagiersvaartuigen_gebied
WHERE ID = identifier;
COMMIT;

/* TERRASSEN: add title column  */
WITH precariobelasting_terrassen_gebied as (
select 
ID identifier, 
/* based on the rate ('tarief') value, a area ('gebied') is identified by an added group number */
'Precariobelasting terrassen per belastinggebied, per jaar, per seizoen en per m2' as title_text
from precariobelasting_terrassen
)
UPDATE precariobelasting_terrassen
SET title = title_text
FROM precariobelasting_terrassen_gebied
WHERE ID = identifier;
COMMIT;