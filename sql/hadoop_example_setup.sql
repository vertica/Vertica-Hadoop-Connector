--the statements below creates and load data into 'allTypes' table which is needed by hadoop-vertica-example.jar
CREATE TABLE allTypes (key identity,
                       intcol integer,
                       floatcol float,
                       charcol char(10),
                       varcharcol varchar,
                       boolcol boolean,
                       datecol date,
                       timestampcol timestamp,
                       timestampTzcol timestamptz,
                       timecol time,
                       timeTzcol timetz,
                       varbincol varbinary,
                       bincol binary,
                       numcol numeric(38,0),
                       intervalcol interval
                        );

copy allTypes column option (varbincol format 'hex', bincol format 'hex') from STDIN direct;
1|-1.11|one|ONE|1|1999-01-08|1999-02-23 03:11:52.35|1999-01-08 07:04:37|07:09:23|15:12:34 EST|0xabcd|0xabcd|1234532|03:03:03
2|+222.22|two|TWO|0|2004-01-09|1999/3/15 05:04|3176/1/18 09:13:00|08:10|17:09 EST|0x1ade|0xade|23487203948283520375|1245:22:34
3|333.0|three|THREE|1|3004/8/1/|1999-03-02 04:01:52|2007-10-09 07:09:23|09:09:08|07:09:14 EST|0xdead|0xdea|+8834534534|0:33
4|0|four|FOUR|0|2004/05/06|2006/02/18 01:02:03|1999-01-08 09:07:00|09:18:00|08:10:15 EST|0xbeefdead|0xbeefdead|-8873445|16:39:01
5|+0|five|FIVE|1|2001/02/03|2006-03-09 12:15:00|1957-03-02 12:14:00|06:07:00|11:23:44 EST|0xbee1aefaf|0xaef|2342234|4:34:22
6|-0|six|SIX|0|1909-09-08|1999-05-04 03:23:52.35|2007-03-06 15:23:29.12345|03:23:32.35|11:34:35 EST|0x979ad||9978662||
7|0.00|seven|SEVEN|1|1959-04-08|2009-11-09 11:32:45|1957-03-02 12:14:00|11:32:45|11:54:23 EST|0xaa|0xaa||13:33:55
8|1E+36|eight|EIGHT|0|2004-10-12|3004/01/08 09:02:41|1957-03-02 12:14:00|11:32:45|00:00:00 EST||0xabc|123|323:44:21
9|-1E+36|nine|NINE|1|2006-11-07|2004-12-25 12:34:12.123456|1957-03-02 12:14:00|07:09:23|23:59:59 EST|0xd|0xd|1|34:33:11
10||ten|TEN|0|1999-01-08|3045-11-28 15:34:59.12345|1957-03-02 12:14:00|12:23:34.123456|15:12:34 EST|0xaa|0xaa|329482903|05:05:05
|11.11|eleven|ELEVEN|1|2007-09-07|1923-10-24 12:23:34.123456|1957-02-03 12:14:00|14:56:04.12345|15:12:34 EST|0xfffff|0xfffff|123172387123123|2344:33
12|12.1212||TWELVE|0|1999-01-08|1923-10-24 12:23:34.1|1957-02-03 12:23:43|11:32:45|15:12:34 EST|0x0000|0x0000|00000000000000000000001|234:22
13|13.1313|thirteen|THIRTEEN||1997-07-03|1923-10-24 12:23:34.123456|1957-02-03 12:14:00|08:09:21|15:12:34 EST|0x1234567890abcdef|0x980|234298347289347|1:1:23
14|14.1414|fourteen||0|1999-02-20|1923-10-24 12:23:34.12|1957-02-03 12:14:00|00:00:00.000000|15:12:34 EST|0xeee|0xeee|1231231787755|2:2:2
15|15.1515|fifteen|FIFTEEN|1||1999-01-01 00:00:00|1957-02-03 12:14:00|23:59:59.999999|15:12:34 EST|0xbe43|0xbe4|123912039123123|0:0:1
16|.00000000000001|sixteen|SIXTEEN|0|2005-01-09||1957-03-02 12:14:00|03:23:32.35|15:12:34 EST|0xaabbccddeeff|0xaabbccdd|0993472340234|33:44:55
