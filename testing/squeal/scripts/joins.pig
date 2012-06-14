fs -copyFromLocal $script/foo.log joins/foo.log
fs -copyFromLocal $script/bar.log joins/bar.log
w = LOAD 'joins/foo.log' USING PigStorage('|') as (a:int,b:int,c:int,d:chararray);
x = LOAD 'joins/foo.log' USING PigStorage('|') as (a:int,b:int,c:int,d:chararray);
y = LOAD 'joins/bar.log' USING PigStorage('\t') as (d:int,e:int);
z = JOIN x by a LEFT OUTER, w by b;
z2 = JOIN z by w::a, y by d;
STORE z2 INTO 'joins/zzz';
cat joins/zzz;
fs -rmr joins;
