    --Tabla con la informacion de las Compañias solo en BD_Central
drop table IF EXISTS companys;
    CREATE TABLE companys(
    id_company integer NOT NULL,
    Nombre             character varying(60) NOT NULL,
    db_instance        character varying(30) NOT NULL,
    db_central character varying(1) default 'N',
    host       character varying,
    schema_name  character varying,   
    CONSTRAINT comp_pkey PRIMARY KEY (id_company)
);
-- Table: usuarios
 DROP TABLE if exists :sch.usuarios;

CREATE TABLE :sch.usuarios
(
    id_company integer NOT NULL,
    id integer NOT NULL,
    nombre character varying(60)  NOT NULL,
    apellido character varying(60) NOT NULL,
    created timestamp,
    updated timestamp,
    synced timestamp,
    updated_function character varying(60),
    CONSTRAINT usuarios_pk PRIMARY KEY (id_company,id)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usuarios
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios
    IS 'Tabla de usuarios';

-- Table: Origen.usu_audit

 DROP TABLE if exists :sch.usuarios_log;

CREATE TABLE :sch.usuarios_log
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    db_instance        character varying(30) NOT NULL,
    secuencia serial NOT NULL,
    id_company integer NOT NULL,
    id integer NOT NULL,
    nombre character varying(60) NOT NULL,
    apellido character varying(60) NOT NULL,
    created timestamp,
    updated timestamp,
    synced timestamp,
    updated_function character varying(60),
    CONSTRAINT usuaud_pk PRIMARY KEY (id_company,id,stamp,secuencia)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usuarios_log
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios_log
    IS 'Tabla de auditoria de usuarios por registro';

-- Table: sch.usuarios_log_col Auditoria por columna
DROP TABLE if exists :sch.usuarios_log_col;

CREATE TABLE :sch.usuarios_log_col
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    db_instance        character varying(30) NOT NULL,
    secuencia SERIAL NOT NULL,
    id_company integer NOT NULL,
    id integer NOT NULL,
    campo VARCHAR NOT NULL,
    valor VARCHAR NULL,
    synced timestamp,
    CONSTRAINT usulogcol_pk PRIMARY KEY (id_company,id,stamp,secuencia)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usuarios_log_col
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios_log_col
    IS 'Tabla de auditoria de usuarios por columna';    

-- Table: facturacion
DROP TABLE IF EXISTS :sch.facturacion;
CREATE TABLE :sch.facturacion
(
    id_company integer NOT NULL,
    date date NOT NULL,
    concept integer NOT NULL,
    invoice_value money,
    synced timestamp,
    updated_function character varying(60),
    CONSTRAINT fact_pkey PRIMARY KEY (id_company, date, concept)	
);
DROP TABLE IF EXISTS :sch.facturacion_log;
CREATE TABLE :sch.facturacion_log
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    db_instance        character varying(30) NOT NULL,
    secuencia serial NOT NULL,
    id_company integer NOT NULL,
    date date NOT NULL,
    concept integer NOT NULL,
    invoice_value money,
    synced timestamp,
    updated_function character varying(60),
    CONSTRAINT logfact_pkey PRIMARY KEY (id_company, date, concept, secuencia)	
);

-- Table: sch.facturacion_log_col Auditoria por columna
DROP TABLE if exists :sch.facturacion_log_col;

CREATE TABLE :sch.facturacion_log_col
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    db_instance        character varying(30) NOT NULL,
    secuencia SERIAL NOT NULL,
    id_company integer NOT NULL,
    date date NOT NULL,
    concept integer NOT NULL,
    campo VARCHAR NOT NULL,
    valor VARCHAR NULL,
    synced timestamp,
    CONSTRAINT faclogcol_pk PRIMARY KEY (id_company,date, concept, stamp,secuencia)
)
TABLESPACE pg_default;

ALTER TABLE :sch.facturacion_log_col
    OWNER to postgres;

COMMENT ON TABLE :sch.facturacion_log_col
    IS 'Tabla de auditoria de usuarios por columna';    
--Tabla para guardar el log de sincronizaciones
DROP TABLE IF EXISTS :sch.Sync_Procs_Log;
CREATE TABLE :sch.Sync_Procs_Log
(
    id_company integer NOT NULL,
    Schema_table character varying(60) NOT NULL,
    Table_name character varying(60) NOT NULL,
    Sync_Type char(1) NOT NULL CHECK (Sync_Type IN ('F','O','D')), --F Full O Origen D Destino
    stamp timestamp NOT NULL,
    user_proc text NOT NULL,
    CONSTRAINT SyncProc_pkey PRIMARY KEY (id_company, Schema_table, Table_name, Sync_Type, stamp)
    --CONSTRAINT type_check );	
);
