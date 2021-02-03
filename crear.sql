--Script general para crear la BD de un cliente y todos los objetos necesarios para la sincronizacion con la BD Central

--Crear la BD para la Compañia
\i crearBD.sql
--cambiar a la BD nueva
\c :BD

--Crear esquema
\i cschema.sql

--Crear funciones DBLINK
CREATE EXTENSION dblink;

--VIsta para ver la informacion de las compañias

--Crear las tablas
\i ctablas.sql
--Crear registros de las comoañias

--Crear la funcion de sincronizacion

--Crear triggers para la auditoria de las tablas
