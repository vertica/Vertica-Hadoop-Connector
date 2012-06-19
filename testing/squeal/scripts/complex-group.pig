fs -copyFromLocal $script/url.log compgroup/url.log
A = LOAD 'compgroup/url.log' USING PigStorage('|') AS (url:chararray,outlink:chararray);
B = GROUP A BY url;
X = FOREACH B {
        FA = FILTER A BY outlink == 'www.xyz.org';
        PA = FA.outlink;
        DA = DISTINCT PA;
	FA2 = FILTER A BY outlink == 'www.cvn.org';
	PA2 = FA2.outlink;
        GENERATE group, COUNT(DA), COUNT(PA2);
};
STORE X INTO 'compgroup/output';
cat compgroup/output;
fs -rmr compgroup;
/*

# select url,count(*), count(distinct outlink) from B group by url where outlink == 'www.xyz.org';
#
# 1 1
# 1 -1
C = FOREACH B GENERATE 1, group, A;

A = LOAD 'data' AS (f1:int,f2:int);
B = GROUP A BY f1;
X = FOREACH B {
  PA = A.f2;
  DA = DISTINCT PA;
  GENERATE group, SUM(DA.f2*DA.f2);
};
*/