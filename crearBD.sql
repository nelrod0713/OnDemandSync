--Crear la BD
-- Crear BD
   CREATE DATABASE :BD
    WITH 
    OWNER = postgres
   TEMPLATE = template0 
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf-8'
    LC_CTYPE = 'en_US.utf-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

