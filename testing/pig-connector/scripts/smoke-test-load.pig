register $pvjar;
register $jdbcjar;

A = load 'sql://{select key, varcharcol from allTypes}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
B = load 'sql://{select key, varcharcol from allTypes where key = ?};{\'1\',\'4\'}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
C = load 'sql://{select key, varcharcol from allTypes where key = ?};sql://{select distinct key from allTypes}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port','$user','');

U = UNION A,B,C;
CORD = ORDER U BY * ASC;
store CORD into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;

X = load 'sql://{select i from empty_table}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
Y = load 'sql://{select i from empty_table where i = ?};{\'1\',\'4\'}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port', '$user', '');
Z = load 'sql://{select i from empty_table where i = ?};{select distinct i from empty_table}' using com.vertica.pig.VerticaLoader('$host', '$dbname', '$port','$user','');
ZYX = UNION X,Y,Z;
ZORD = ORDER ZYX BY * ASC;
store ZORD into 'result' using PigStorage('|');
fs -cat result/part*;
fs -rmr result;
