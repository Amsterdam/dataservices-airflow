function publish_table {
	# $1 is target table base name, e.g. milieuzones
	# The variablesmileuzones_new and mileuzones_old are derived

	TABLE_TARGET=$1
	TABLE_NEW=${1}_new
	TABLE_OLD=${1}_old

  # Changes _new to target database and alters standard indices create by "ogr2ogr -f PGDump"
	psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS $TABLE_TARGET RENAME TO $TABLE_OLD;
ALTER TABLE $TABLE_NEW RENAME TO $TABLE_TARGET;
DROP TABLE IF EXISTS $TABLE_OLD;

ALTER INDEX ${TABLE_NEW}_pk RENAME TO ${TABLE_TARGET}_pk;
ALTER INDEX ${TABLE_NEW}_wkb_geometry_geom_idx RENAME TO ${TABLE_TARGET}_wkb_geometry_geom_idx;

COMMIT;
SQL

}
