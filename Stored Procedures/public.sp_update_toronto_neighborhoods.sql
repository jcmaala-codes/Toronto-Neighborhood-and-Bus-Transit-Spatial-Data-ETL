-- PROCEDURE: public.sp_update_toronto_neighborhoods()

-- DROP PROCEDURE IF EXISTS public.sp_update_toronto_neighborhoods();

CREATE OR REPLACE PROCEDURE public.sp_update_toronto_neighborhoods()
LANGUAGE 'plpgsql'
AS $BODY$

BEGIN

	RAISE NOTICE 'TRUNCATING TABLE public.toronto_neighborhoods';

	--TRUNCATE CONTENTS OF TABLE
	TRUNCATE TABLE public.toronto_neighborhoods  RESTART IDENTITY;

	--LOAD ALL DATA
	RAISE NOTICE 'INSERTING DATA TO public.toronto_neighborhoods';
	WITH rows as (
		INSERT INTO public.toronto_neighborhoods(
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
		FROM "raw"."toronto_neighborhoods"
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
		CAST('sp_update_toronto_neighborhoods' as VARCHAR)
		, now()
		, CAST('toronto_neighborhoods' as VARCHAR)
		, CAST('INSERT DATA' as VARCHAR)
		, COUNT(*) as rowsaffected
	FROM rows;

	RAISE NOTICE 'DONE INSERTING DATA TO public.toronto_neighborhoods';

EXCEPTION
	WHEN OTHERS THEN 
	RAISE WARNING 'An error occurred: %', SQLERRM;
	ROLLBACK;
	
END;
$BODY$;
ALTER PROCEDURE public.sp_update_toronto_neighborhoods()
    OWNER TO postgres;
