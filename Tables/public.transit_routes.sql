-- Table: public.transit_routes

-- DROP TABLE IF EXISTS public.transit_routes;

CREATE TABLE IF NOT EXISTS public.transit_routes
(
    id SERIAL,
    "Name" text COLLATE pg_catalog."default",
    "Description" text COLLATE pg_catalog."default",
    geometry geometry(Geometry,4326),
    CONSTRAINT transit_routes_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transit_routes
    OWNER to postgres;
-- Index: idx_public_transit_routes_geometry

-- DROP INDEX IF EXISTS public.idx_public_transit_routes_geometry;

CREATE INDEX IF NOT EXISTS idx_public_transit_routes_geometry
    ON public.transit_routes USING gist
    (geometry)
    TABLESPACE pg_default;