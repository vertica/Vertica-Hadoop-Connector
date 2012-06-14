register $pvjar;
register $jdbcjar;

CC = load 'sql://{select * from ints}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$userwithpassword', '$passwd');
store CC into 'result' using PigStorage('|');
