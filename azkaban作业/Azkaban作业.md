# Azkaban作业

现有用户点击行为数据文件，每天产生会上传到hdfs目录，按天区分目录，现在我们需要每天凌晨两点定时导入Hive表指定分区中，并统计出今日活跃用户数插入指标表中。

日志文件

clicklog

```txt
userId   click_time             index
uid1	2020-06-21	12:10:10	a.html 
uid2	2020-06-21	12:15:10	b.html 
uid1	2020-06-21	13:10:10	c.html 
uid1	2020-06-21	15:10:10	d.html 
uid2	2020-06-21	18:10:10	e.html
```

用户点击行为数据，三个字段是用户id,点击时间，访问页面

hdfs目录会以日期划分文件，例如：

```shell
/user_clicks/20200621/clicklog.dat
/user_clicks/20200622/clicklog.dat
/user_clicks/20200623/clicklog.dat
...
```



Hive表

原始数据分区表

```shell
create table user_clicks(id string,click_time string index string) partitioned by(dt string) row format delimited fields terminated by '\t' ;
```

需要开发一个import.job每日从hdfs对应日期目录下同步数据到该表指定分区。（日期格式同上或者自定义）

指标表

```shell
create table user_info(active_num string,date string) row format delimited fields terminated by '\t' ;
```

需要开发一个analysis.job依赖import.job执行，统计出每日活跃用户(一个用户出现多次算作一次)数并插入user_inof表中。


作业：

开发以上提到的两个job，job文件内容和sql内容需分开展示，并能使用azkaban调度执行。



--建表语句
drop table user_clicks;
drop table user_info;
create    table user_clicks(id string,click_time string,index string)partitioned by(dt string) row format delimited fields terminated by '\t' ;
create    table user_info(active_num string,datetime string) row format delimited fields terminated by '\t' ;
--把日志信息添加到hdfs
hdfs dfs -put clicklog.dat /user_clicks/20200909/
--开始尝试直接获取hive的系统时间发现不行
load data  inpath '/user_clicks/20200909/clicklog.dat' into table user_clicks;
load data  inpath '/user_clicks/'||date_format(current_date(), 'yyyyMMdd')||'/clicklog.dat' into table user_clicks;
hive -e "load data  inpath '/user_clicks/20200909/clicklog.dat' into table mode2test.user_clicks partition(dt='2020-09-09');"