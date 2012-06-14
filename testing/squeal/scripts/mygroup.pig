x = LOAD 'scripts/foo.log' USING PigStorage('\t') as (a:int,b:int,c:int);
y = GROUP x BY b;
z = FOREACH y {
  f = FILTER x BY c > 5;
  GENERATE group, SUM(f.a);
};
STORE z INTO 'zzz';
