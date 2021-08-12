create or replace procedure carga_licitacion()
language plpgsql
as $$ 
begin 
	delete from public.licitacion where nombre_archivo in (select distinct nombre_archivo from public.licitacion_tmp);

	insert into public.licitacion 
	select *, CURRENT_TIMESTAMP from public.licitacion_tmp;
	
	truncate table public.licitacion_tmp;
end; $$
