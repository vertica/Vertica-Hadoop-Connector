x = LOAD 'scripts/foo.log' USING PigStorage('\t') as (a:int,b:int,c:int);
y = FOREACH x GENERATE COUNT(a);
z = FOREACH y GENERATE $0+1;
STORE z INTO 'data';
