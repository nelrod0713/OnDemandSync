--Crear registros de las comoañias
INSERT INTO public.companys(
	id_company, nombre, db_instance, db_central, host, schema_name)
	VALUES (0, 'BD de Administración', 'bd_ori', 'S','192.168.1.102','ori');

INSERT INTO public.companys(
	id_company, nombre, db_instance, db_central, host, schema_name)
	VALUES (1, 'BD Cliente1', 'bd_des', 'N','192.168.1.102','des');

