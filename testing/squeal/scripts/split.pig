x = LOAD 'scripts/foo.log' USING PigStorage('\t') as (a:int,b:int,c:int);
STORE x INTO 'yyy';
STORE x INTO 'zzz';
