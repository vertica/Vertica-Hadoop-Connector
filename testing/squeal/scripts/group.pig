fs -rmr group
fs -copyFromLocal $script/foo.log group/foo.log
x = LOAD 'group/foo.log' USING PigStorage('|') as (a:int,b:int,c:int,d:chararray);
y = GROUP x BY b;
z = FOREACH y GENERATE group, COUNT(x);
STORE z INTO 'group/zzz';
cat group/zzz;
fs -rmr group

