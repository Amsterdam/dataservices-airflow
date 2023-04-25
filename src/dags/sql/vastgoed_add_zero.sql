--Convert pand_id and verblijfsobject_id from Integer to String
alter table 	public.vastgoed_vastgoed
alter column 	pand_id 				type varchar,
alter column 	verblijfsobject_id 		type varchar;

--Update pand_id and verblijfsobject_id with "voorloopnul"
UPDATE 			public.vastgoed_vastgoed
SET 	pand_id = lpad(pand_id, 16, '0');

--Update verblijfsobject_id with "voorloopnul"
UPDATE 			public.vastgoed_vastgoed
SET 	verblijfsobject_id = lpad(verblijfsobject_id, 16, '0');