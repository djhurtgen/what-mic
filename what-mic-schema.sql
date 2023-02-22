-- This script was generated by the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public."factResults"
(
    fact_id integer NOT NULL,
    mic_id integer NOT NULL,
    band_id integer NOT NULL,
    venue_id integer NOT NULL,
    source_id integer NOT NULL,
    result numeric(4, 2) NOT NULL,
    PRIMARY KEY (fact_id)
);

CREATE TABLE IF NOT EXISTS public."dimMics"
(
    mic_id integer NOT NULL,
    manufacturer "char" NOT NULL,
    model "char" NOT NULL,
    type "char" NOT NULL,
    PRIMARY KEY (mic_id)
);

CREATE TABLE IF NOT EXISTS public."dimBand"
(
    band_id integer NOT NULL,
    band_name "char" NOT NULL,
    num_members smallint NOT NULL,
    style "char" NOT NULL,
    PRIMARY KEY (band_id)
);

CREATE TABLE IF NOT EXISTS public."dimVenue"
(
    venue_id integer NOT NULL,
    venue_name "char" NOT NULL,
    size "char" NOT NULL,
    reverberance "char" NOT NULL,
    PRIMARY KEY (venue_id)
);

CREATE TABLE IF NOT EXISTS public."dimSource"
(
    source_id integer NOT NULL,
    source_type "char" NOT NULL,
    PRIMARY KEY (source_id)
);

ALTER TABLE IF EXISTS public."factResults"
    ADD FOREIGN KEY (mic_id)
    REFERENCES public."dimMics" (mic_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."factResults"
    ADD FOREIGN KEY (band_id)
    REFERENCES public."dimBand" (band_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."factResults"
    ADD FOREIGN KEY (venue_id)
    REFERENCES public."dimVenue" (venue_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."factResults"
    ADD FOREIGN KEY (source_id)
    REFERENCES public."dimSource" (source_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;