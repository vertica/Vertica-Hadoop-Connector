register $pvjar;
register $jdbcjar;

X = load 'ints.dat' using PigStorage('|') as (a: long, b: chararray);
STORE X into '{open.ints}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$userwithpassword', 'abc');
