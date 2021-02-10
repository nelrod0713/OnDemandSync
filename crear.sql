--Script general para crear la BD de un cliente y todos los objetos necesarios para la sincronizacion ON DEMAND 
--Para correr el script "crear.sql" desde la terminal
--psql -U postgres -v BD="bd_ori" -v sch="ori" < crear.sql
--Crear la BD para la Compañia
\i crearBD.sql
--cambiar a la BD nueva
\c :BD

--Crear esquema
\i cschema.sql

--Crear funciones DBLINK
CREATE EXTENSION dblink;

--VIsta para ver la informacion de las compañias
\i cvista.sql

--Crear las tablas
\i ctablas.sql

--Crear la funcion de sincronizacion
\i Fu_AuditLog.sql
--Crear triggers para la auditoria de las tablas
\i ctriger.sql
