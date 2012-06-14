register $pvjar;
register $jdbcjar;

fs -copyFromLocal $script/smoke-test.dat smoke-test.dat;
fs -copyFromLocal $script/ints.dat ints.dat;

E = load 'smoke-test.dat' using PigStorage('|') as (a: int, b: chararray, c: chararray, d: chararray, f: double, n: double, t: chararray, v: chararray, z: bytearray);
STORE E into '{mrtarget(a int,b boolean,c char(1),d date,f float,n numeric,t timestamp,v varchar,z varbinary)}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

B = load 'sql://{select * from mrtarget}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store B into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

F = load 'ints.dat' using PigStorage('|') as (a: long, b: chararray);
STORE F into '{ints_id}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

C = load 'sql://{select a,b from ints_id}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store C into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

FD = load 'ints.dat' using PigStorage('|') as (a: long, b: chararray);
STORE FD into '{$dbname.open.ints}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

CD = load 'sql://{select a,b from $dbname.open.ints}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store CD into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

-- Try to load into a bogus table without specifing a schema
STORE E into '{bogus}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

