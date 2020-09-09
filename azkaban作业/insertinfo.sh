#!bin/bash
hql="insert into table mode2test.user_info select count(distinct id),'$(date +%Y-%m-%d)' from mode2test.user_clicks where dt=='$(date +%Y-%m-%d)';"
hive -e "$hql"