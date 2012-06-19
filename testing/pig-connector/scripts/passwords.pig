register $pvjar;
register $jdbcjar;

B = load 'sql://{select key, varcharcol from open.allTypes}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$userwithpassword', '$passwd');
CORD = ORDER B BY * ASC;
store CORD into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

F = load 'ints.dat' using PigStorage('|') as (a: long, b: chararray);
STORE F into '{open.ints}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$userwithpassword', '$passwd');

C = load 'sql://{select * from open.ints}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$userwithpassword', '$passwd');
store C into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;
