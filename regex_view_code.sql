CREATE OR REPLACE VIEW dev.meta_col_datatype_v2 AS 
SELECT columns.table_schema,
	columns.table_name,
	AS "position",
	columns.column_name,
	AS current_datetype,
	(((((((((((((((((((((((((((((((((((((((((((((concat(((((
	'SELECT *, case when datatype like ''varchar%'' then ''e.'
	' || column_name when datatype like ''boolean%'' then ''e.'
	' || column_name || ''::boolean'' when datatype like ''int%'' then ''e.'
	' || column_name || ''::int'' when datatype like ''numeric%'' then ''e.'
	' || column_name || ''::numeric(18,4)'' when datatype like ''timestamp%'
	' then ''cast(e.'' || column_name || '' as timestamp)'
	' end merge_cast  from (SELECT table_name, "position", column_name, data_type current_datetype, is_true_false, max_col_len, case when is_not_null = 0 then '
	'Y'' else ''N'
	' end all_nulls, isnull(case when is_true_false > 0 and is_true_false >= is_string then '
	'boolean'
	' when (is_date > 0 and is_date >= is_string) or (is_timestamp  > 0 and is_timestamp >= is_string) or (is_timestampz > 0 and is_timestampz >= is_string) then '
	'timestamp'
	' when is_int > 0 and is_decimal = 0 and is_date = 0 and is_timestamp = 0 /*and is_timestampz = 0 */ and is_alphabet = 0 then '
	'int'' when is_decimal > 0 and is_decimal >= is_string_special_char then '
	'numeric(18,4)'' when is_string > 0 or is_string_special_char > 0 then '
	'varchar(''|| data_length || '')'' end, ''varchar(250)'
	') datatype, case when cnt = is_not_null then '' not null'' else '''
	' end nullable, is_int, is_decimal, is_date, is_timestamp, is_timestampz, is_string, is_alphabet, is_string_special_char from ( SELECT '
	''::varchar(1306) || columns.table_name) || ''' table_name, '''::varchar(15)
	) || columns.data_type) || ''' data_type, '::varchar(13)),
	(columns.ordinal_position)::varchar) || ' "position", '''::varchar(14)) || 
	columns.column_name) || ''' column_name, count(1) cnt, max(length('::varchar
	(40)) || columns.column_name) || 
	'::varchar)) max_col_len, case when max(length('::varchar(46)) || columns.
	column_name) || '::varchar)) < 20 then 20 when max(length('::varchar(41)) || 
	columns.column_name) || 
	'::varchar)) between 21 and 40 then 50 when max(length('::varchar(54)) || 
	columns.column_name) || 
	'::varchar)) between 41 and 70 then 100 when max(length('::varchar(55)) || 
	columns.column_name) || 
	'::varchar)) between 71 and 200 then 250 when max(length('::varchar(56)) || 
	columns.column_name) || 
	'::varchar)) between 201 and 400 then 500 when max(length('::varchar(57)) || 
	columns.column_name) || 
	'::varchar)) between 401 and 700 then 1000 when max(length('::varchar(58)) 
	|| columns.column_name) || 
	'::varchar)) between 701 and 1700 then 2000 when max(length('::varchar(59)) 
	|| columns.column_name) || 
	'::varchar)) between 1701 and 3500 then 4000 else 8000 end data_length, COUNT(CASE WHEN REGEXP_COUNT('
	::varchar(100)) || columns.column_name) || ',''^[a-zA-Z0-9_]*$'
	')>0 then 1 end) is_string, COUNT(CASE WHEN REGEXP_COUNT('::varchar(74)) || 
	columns.column_name) || ',''^.*[a-zA-Z].*$'
	') > 0 then 1 end) is_alphabet, COUNT(CASE WHEN REGEXP_COUNT('::varchar(77)) 
	|| columns.column_name) || E',''[^a-zA-Z0-9\\s]'
	') >0 then 1 end) is_string_special_char, COUNT(CASE WHEN REGEXP_COUNT('::
	varchar(87)) || columns.column_name) || ',''^[0-9]*$'
	') > 0 then 1 end) is_int, COUNT(CASE WHEN REGEXP_COUNT('::varchar(66)) || 
	columns.column_name) || E',''(\\+|-)?([0-9]+(\\.[0-9]+))'
	') > 0 then 1 end) is_decimal, COUNT(CASE WHEN REGEXP_COUNT('::varchar(87)) 
	|| columns.column_name) || E','
	'^(19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])$'
	') > 0 then 1 end) is_date, COUNT(CASE WHEN REGEXP_COUNT('::varchar(121)) || 
	columns.column_name) || E','
	'^(19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])$'
	') > 0 then 1 end) is_timestamp, COUNT(CASE WHEN REGEXP_COUNT('::varchar(126
	)) || columns.column_name) || E','
	'^(\\d{4}((-)?(0[1-9]|1[0-2])((-)?(0[1-9]|[1-2][0-9]|3[0-1])(T(24:00(:00(\\.[0]+)?)?|(([0-1][0-9]|2[0-3])(:)[0-5][0-9])((:)[0-5][0-9](\\.[\\d]+)?)?)((\\+|-)(14:00|(0[0-9]|1[0-3])(:)[0-5][0-9])|Z))?)?)?)$'
	')>0 then 1 end) is_timestampz, COUNT(CASE WHEN lower('::varchar(253)) || 
	columns.column_name) || ') in (''true'',''false'
	') then 1 end ) is_true_false, COUNT(CASE WHEN '::varchar(66)) || columns.
	column_name) || ' is not null then 1 end ) is_not_null from '::varchar(43)) 
	|| columns.table_schema) || '.'::varchar(1)) || columns.table_name) || 
	') x) x2 union all'::varchar(17)) AS meta_query 
FROM v_catalog.columns 
ORDER BY columns.ordinal_position
GO

select meta_query from dev.meta_col_datatype_v2 where table_name  = 'qe_raw_event';


