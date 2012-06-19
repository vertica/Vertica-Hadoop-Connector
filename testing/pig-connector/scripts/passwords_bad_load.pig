register $pvjar;
register $jdbcjar;
T = load 'sql://{select key, varcharcol from open.allTypes}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$userwithpassword', 'abc');
dump T;
