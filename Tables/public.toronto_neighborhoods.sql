-- Table: raw.toronto_neighborhoods

-- DROP TABLE IF EXISTS public.toronto_neighborhoods;

CREATE TABLE IF NOT EXISTS public.toronto_neighborhoods
(
    id SERIAL,
    "Name" text COLLATE pg_catalog."default",
    "Description" text COLLATE pg_catalog."default",
    geometry geometry(Geometry,4326),
    CONSTRAINT toronto_neighborhoods_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.toronto_neighborhoods
    OWNER to postgres;
-- Index: idx_public_toronto_neighborhoods_geometry

-- DROP INDEX IF EXISTS public.idx_public_toronto_neighborhoods_geometry;

CREATE INDEX IF NOT EXISTS idx_public_toronto_neighborhoods_geometry
    ON public.toronto_neighborhoods USING gist
    (geometry)
    TABLESPACE pg_default;