-- PROCEDURE: public.sp_update_transit_routes()

-- DROP PROCEDURE IF EXISTS public.sp_update_transit_routes();

CREATE OR REPLACE PROCEDURE public.sp_update_transit_routes()
LANGUAGE 'plpgsql'
AS $BODY$

BEGIN

	RAISE NOTICE 'TRUNCATING TABLE public.transit_routes';

	--TRUNCATE CONTENTS OF TABLE
	TRUNCATE TABLE public.transit_routes  RESTART IDENTITY;

	--LOAD ALL DATA
	RAISE NOTICE 'INSERTING DATA TO public.transit_routes';
	WITH rows as (
		INSERT INTO public.transit_routes(
			"Name",
			"Description",
			"geometry"
		) 
		SELECT 
		"Name",
		"Description",
			ST_Force2D( 
				ST_Transform(
					ST_SetSRID( 
						CASE WHEN ST_IsValid(geometry) 
						THEN geometry 
						ELSE ST_MakeValid(geometry) END , 
						4326), 
				4326) 
			) as geometry
		FROM "raw"."transit_routes"
		ORDER BY "Name"
		RETURNING 1
	) 
	INSERT INTO public.spruntimelogs 
		(storedprocedure
		, runtime
		, tablename
		, action
		, affectedrows) 
	SELECT 
		CAST('sp_update_transit_routes' as VARCHAR)
		, now()
		, CAST('transit_routes' as VARCHAR)
		, CAST('INSERT DATA' as VARCHAR)
		, COUNT(*) as rowsaffected
	FROM rows;

	RAISE NOTICE 'DONE INSERTING DATA TO public.transit_routes';

EXCEPTION
	WHEN OTHERS THEN 
	RAISE WARNING 'An error occurred: %', SQLERRM;
	ROLLBACK;
	
END;
$BODY$;
ALTER PROCEDURE public.sp_update_transit_routes()
    OWNER TO postgres;
