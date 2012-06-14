register $pvjar;
register $jdbcjar;

-- Insert into an existing table - VER-17113.
E = load 'smoke-test.dat' using PigStorage('|') as (a: int, b: chararray, c: chararray, d: chararray, f: double, n: double, t: chararray, v: chararray, z: bytearray);
STORE E into '{mrtarget}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

B = load 'sql://{select * from mrtarget}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store B into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

STORE E into '{hadoop.mrtarget(a int,b boolean,c char(1),d date,f float,n numeric,t timestamp,v varchar,z varbinary)}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');
B = load 'sql://{select * from hadoop.mrtarget}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store B into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

STORE E into '{hadoop.existing}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');
B = load 'sql://{select * from hadoop.existing}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store B into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

-- VER-19219
F = load 'ints.dat' using PigStorage('|') as (a: long, b: chararray);
STORE F into '{ints}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');

C = load 'sql://{select * from ints}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
store C into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

-- VER-17116 - handle schemas correctly. Load into an unknown schema & an existing schema
STORE E into '{brandnewschema.mrtarget(a int,b boolean,c char(1),d date,f float,n numeric,t timestamp,v varchar,z varbinary)}' using com.vertica.pig.VerticaStorer('$host', '$dbname', '$port', '$user', '');


