CREATE SCHEMA IF NOT EXISTS prime AUTHORIZATION postgres;

GRANT ALL ON SCHEMA prime TO postgres;

ALTER ROLE postgres SET search_path TO prime, public;
