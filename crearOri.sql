--Script general para crear la BD de un cliente y todos los objetos necesarios para la sincronizacion ON DEMAND 
--Para correr el script "crear.sql" desde la terminal
--psql -U postgres -v BD="bd_ori" -v sch="ori" < crearOri.sql
--Crear la BD para la Compañia
\i crearBD.sql
--cambiar a la BD nueva
\c :BD

--Crear esquema
\i cschema.sql

--Crear funciones DBLINK
CREATE EXTENSION dblink;

-- Crear sequences 
\i Cseqs.sql
--Crear las tablas
\i ctablas.sql

--VIsta para ver la informacion de las compañias
\i cvistaOri.sql
\i Icias.sql
-- Crear sequences 
--\i Cseqs.sql

--Crear la funciones de sincronizacion
\i Fu_AuditLog.sql
\i Fu_ProcsLog.sql
\i Fu_DesDbSync.sql
\i Fu_OriDbSync.sql
\i Fu_DesReg.sql
\i Fu_OriReg.sql
\i Fu_FullReg.sql

--Crear triggers para la auditoria de las tablas
\i ctriger.sql
