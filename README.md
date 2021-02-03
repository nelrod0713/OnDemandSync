"# -OnDemandSync" 
--Para correr el script "Ctab.sql" desde la terminal
psql -U postgres -v bd="bd" -v sqm="ori" < Ctab.sql

--incluir la linea por cada BD en el archivo C:\Program Files\PostgreSQL\13\data\pg_hba.conf del servidor de BD.
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    bd9             postgres        192.168.1.102/32        trust
