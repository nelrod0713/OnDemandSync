--VIsta de registros a sincronizar en Destino
CREATE or replace VIEW ori.v_usuarios_aud_ori AS
--Registros de Insert/ Delete de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.usuarios_log 
where synced is null 
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
from ori.usuarios_log_col 
where synced is null 
union 
--Registros de Update de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros a sincronizar en Origen
CREATE or replace VIEW ori.v_usuarios_aud_des AS
--Registros de Insert/ Delete de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.v_usuarios_log 
where synced is null 
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
from ori.usuarios_log_col 
where synced is null 
union 
--Registros de Update de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta de Facturacion en origen registros a sincronizar en Destino
CREATE or replace VIEW ori.v_facturacion_aud_ori AS
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
 from ori.facturacion_log 
where synced is null 
union 
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
from ori.facturacion_log_col 
where synced is null 
union 
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
from ori.v_facturacion_log_col --_aud_des 
where synced is null 
order by stamp;

