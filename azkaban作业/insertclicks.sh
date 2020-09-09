#!bin/bash
hql="load data  inpath '/user_clicks/$(date +%Y%m%d)/clicklog.dat' into table mode2test.user_clicks partition(dt='$(date +%Y-%m-%d)');"
hive -e "$hql"