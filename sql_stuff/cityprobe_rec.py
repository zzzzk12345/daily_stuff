# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import dataframe
from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
import os
from os.path import basename
import pymysql
import numpy as np
import pandas as pd
import datetime
import time

class MySQLHelper():
    def __init__(self, host, user, password, database='', port=3306, charset='utf8', as_dict=False):
        self.config = {'host': host,
                       'user': user,
                       'password': password,
                       'port': port,
                       'charset': charset,
                       'database': database
                       }

        self.cnn = pymysql.connect(**self.config)
        if as_dict:
            self.cur = self.cnn.cursor(pymysql.cursors.DictCursor)
        else:
            self.cur = self.cnn.cursor()

    def execute(self, sql, args=[]):
        self.cur.execute(sql, args)
        self.cnn.commit()

    def executemany(self, sql, args=[]):
        self.cur.executemany(sql, args)
        self.cnn.commit()

    def fetchone(self, sql, args=[]):
        self.cur.execute(sql, args)
        return self.cur.fetchone()

    def fetchmany(self, sql, args=[], size=None):
        self.cur.execute(sql, args)
        return self.cur.fetchmany(size)

    def fetchall(self, sql, args=[]):
        self.cur.execute(sql, args)
        return self.cur.fetchall()

    def close(self):
        self.cur.close()
        self.cnn.close()


hdfs_prefix = 'hdfs://dankecluster/user/bi_ba_op/'

settings = [
    ("hive.metastore.uris", "thrift://hadoop-master002:9083"),
    ("dfs.nameservices", "dankecluster"),
    ("dfs.ha.namenodes.dankecluster", "nn1,nn2"),
    ("dfs.client.failover.proxy.provider.dankecluster",
     "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"),
    ("dfs.namenode.rpc-address.dankecluster.nn1", "hadoop-master001:8020"),
    ("dfs.namenode.rpc-address.dankecluster.nn2", "hadoop-master002:8020"),
]


class SparkHelper(object):
    def __init__(self, app_name='app-spark-submit'):
        self.spark_conf = SparkConf().setAppName("cityprobe_rec_submit").set("spark.sql.execution.arrow.enabled", "true").setAll(settings)
        self.sparkSession = SparkSession.builder.master("yarn").config(
            conf=self.spark_conf).enableHiveSupport().getOrCreate()

    def read_from_hive(self, sql):
        return self.sparkSession.sql(sql)

    def write_to_hive(self, df, tableName):
        df.write.mode('overwrite').saveAsTable(tableName)

    def write_to_hdfs(self, df: dataframe, filename):
        df.coalesce(1).write.mode('overwrite').csv(hdfs_prefix + filename, header='true')

    def read_from_hdfs(self, filename):
        return self.sparkSession.read.csv(hdfs_prefix + filename, header='true')

    def read_from_mysql_rds(self, sql, database):
        # return self.sparkSession.read.format("jdbc").option("dbtable",sql).option("driver", 'com.mysql.jdbc.Driver').option('url','jdbc:mysql://172.22.222.80/Laputa').load()
        return self.sparkSession.read.format("jdbc").options(
            url="jdbc:mysql://rr-2zetk6oj0m9p6g95j.mysql.rds.aliyuncs.com/%s?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull" % database,
            driver="com.mysql.jdbc.Driver",
            dbtable='(%s) as t' % sql,
            user="bi_lizhao",
            password="yKSNtZM7nqER4E"
        ).load()

    def read_from_mysql_DMADB(self, sql):
        # return self.sparkSession.read.format("jdbc").option("dbtable",sql).option("driver", 'com.mysql.jdbc.Driver').option('url','jdbc:mysql://172.22.222.80/Laputa').load()
        return self.sparkSession.read.format("jdbc").options(
            url="jdbc:mysql://172.22.222.81:3309/DMADB?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull",
            driver="com.mysql.jdbc.Driver",
            dbtable='(%s) as t' % sql,
            user="bi_dma_write",
            password="d6OqLNheW69yU"
        ).load()

    def write_to_mysql_DMADB(self, df, tablename):
        properties = {
            "user": "bi_dma_write",
            "password": "d6OqLNheW69yU"
        }
        return df.coalesce(20).write.mode('append') \
            .jdbc(
            url="jdbc:mysql://172.22.222.81:3309/DMADB?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
            , table=tablename, mode='append', properties=properties)

if __name__ == '__main__':
    time_start_all = time.time()
    spark_helper = SparkHelper()
    mysql_helper = MySQLHelper('172.22.222.81', 'bi_dma_write', 'd6OqLNheW69yU', 'DMADB', 3309)
    # 取地铁id，城市名，商圈名
    df_subways = spark_helper.read_from_mysql_rds("select id, city, name from subways", "Laputa")
    df_subways.createOrReplaceTempView("subways")
    # 取地铁id，地铁线id，地铁顺序
    df_subway_station_link = spark_helper.read_from_mysql_rds("select subway_id, subway_line_id, station_sort_no from subway_station_link", "Laputa")
    df_subway_station_link.createOrReplaceTempView("subway_station_link")
    df_subway_info = spark_helper.read_from_hive('''select a.*,b.* from subways a 
                                                                full outer join subway_station_link b 
                                                                on a.id = b.subway_id''')
    df_subway_info.createOrReplaceTempView("subway_info")
    # 生成基础地铁表
    df_subway_link_info_tmp = spark_helper.read_from_hive("SELECT *, ROW_NUMBER() OVER(PARTITION BY subway_line_id ORDER BY station_sort_no) AS rn,\
                                                LAG(station_sort_no, 1) OVER(PARTITION BY subway_line_id ORDER BY station_sort_no) AS last_1_station, \
                                                LAG(station_sort_no, 2) OVER(PARTITION BY subway_line_id ORDER BY station_sort_no) AS last_2_station, \
                                                LEAD(station_sort_no, 1) OVER(PARTITION BY subway_line_id ORDER BY station_sort_no) AS next_1_station, \
                                                LEAD(station_sort_no, 2) OVER(PARTITION BY subway_line_id ORDER BY station_sort_no) AS next_2_station \
                                                FROM subway_info")
    df_subway_link_info_tmp.createOrReplaceTempView("subway_link_info_tmp")
    df_subway_link_info_union = spark_helper.read_from_hive('''select 0 as level, a.*, a.name as neibor_poi_name_0 from subway_link_info_tmp a \
                                                            union all 
                                                            select 1 as level, a.*,b.name as neibor_poi_name_1 from subway_link_info_tmp a join subway_link_info_tmp b 
                                                            on a.subway_line_id = b.subway_line_id and a.last_1_station = b.station_sort_no where b.name is not null 
                                                            union all 
                                                            select 2 as level, a.*,b.name as neibor_poi_name_2 from subway_link_info_tmp a join subway_link_info_tmp b 
                                                            on a.subway_line_id = b.subway_line_id and a.last_2_station = b.station_sort_no where b.name is not null 
                                                            union all 
                                                            select 1 as level, a.*,b.name as neibor_poi_name_3 from subway_link_info_tmp a join subway_link_info_tmp b 
                                                            on a.subway_line_id = b.subway_line_id and a.next_1_station = b.station_sort_no where b.name is not null 
                                                            union all 
                                                            select 2 as level, a.*,b.name as neibor_poi_name_4 from subway_link_info_tmp a join subway_link_info_tmp b 
                                                            on a.subway_line_id = b.subway_line_id and a.next_2_station = b.station_sort_no where b.name is not null''')
    df_subway_link_info_union.createOrReplaceTempView("subway_link_info_union")
    # 用户偏好表
    df_intentional_user = spark_helper.read_from_mysql_rds("SELECT * FROM intentional_user", "BizopsWH")
    df_intentional_user.createOrReplaceTempView("intentional_user")
    # 用户状态表
    df_intentional_user_status = spark_helper.read_from_mysql_rds(
        "select mobile, recieve_status, status, created_at from intentional_user_status", "BizopsWH")
    df_intentional_user_status.createOrReplaceTempView("intentional_user_status")
    # 有效用户表
    df_effective_users = spark_helper.read_from_hive("select a.*, if(year(split(a.latest_date,' ')[0]) < year(CURRENT_TIMESTAMP),\
                                                concat(cast(date_add(to_date(a.latest_date), 365) as string), ' ', split(a.latest_date,' ')[1]),\
                                                a.latest_date) as latest_date_edit from \
                                                (select * from intentional_user) a join \
                                                (select * from intentional_user_status) b on a.mobile=b.mobile \
                                                where a.mobile != '    ' and b.mobile != '    '\
                                                and b.recieve_status is null and b.status != '不活跃的注册app用户'")
    df_effective_users.createOrReplaceTempView("effective_users")
    # 从Hive读取可租的房源
    df_f_bi_roomforrent_detail = spark_helper.read_from_hive("select * from danke_old_hive.dm__f_bi_roomforrent_detail WHERE p_day = current_date + interval '-1' day")
    df_f_bi_roomforrent_detail.createOrReplaceTempView("room")
    df_room = spark_helper.read_from_hive("select room_id,room_code,suite_id,xiaoqu_id,xiaoqu_name,\
                                        block_id,block,city_name,room_area,face,has_bathroom,has_balcony,suite_status,bedroom_num,\
                                        price,has_month_rent,month_price,ready_for_rent_date,sf_remain_days,\
                                        has_lift,split(subway_lines,';')[0] as subway_line,\
                                        split(subway_name,';')[0] as subway_name,split(subway_distance,';')[0] as subway_distance,\
                                        case when rent_type in ('合租','蛋壳租房_分租') then '合租' else '整租' end as rent_type\
                                        from room")
    df_room.createOrReplaceTempView("rooms")
    # 房源客户总匹配
    df_rooms_users = spark_helper.read_from_hive("select \
                                            u.id as u_id, \
                                            u.user_id as u_user_id, \
                                            u.user_name as u_user_name, \
                                            u.mobile as u_mobile, \
                                            u.city as u_city, \
                                            u.gender as u_gender, \
                                            u.latest_date as u_latest_date, \
                                            u.plan_months as u_plan_months, \
                                            u.app_price as u_app_price, \
                                            u.dx_price as u_dx_price, \
                                            u.price as u_price, \
                                            u.app_poiblock1 as u_app_poiblock1, \
                                            u.app_poiblock2 as u_app_poiblock2, \
                                            u.poiblock1 as u_poiblock1, \
                                            u.poiblock2 as u_poiblock2, \
                                            u.workblock1 as u_workblock1, \
                                            u.workblock2 as u_workblock2, \
                                            u.liveblock1 as u_liveblock1, \
                                            u.liveblock2 as u_liveblock2, \
                                            u.app_rent_type as u_app_rent_type, \
                                            u.dx_rent_type as u_dx_rent_type, \
                                            u.rent_type as u_rent_type, \
                                            u.face as u_face, \
                                            u.has_balcony as u_has_balcony, \
                                            u.app_has_toilet as u_app_has_toilet, \
                                            u.dx_has_toilet as u_dx_has_toilet, \
                                            u.has_toilet as u_has_toilet, \
                                            u.app_near_subway as u_app_near_subway, \
                                            u.dx_near_subway as u_dx_near_subway, \
                                            u.near_subway as u_near_subway, \
                                            u.room_nums as u_room_nums, \
                                            u.has_lift as u_has_lift, \
                                            u.area as u_area, \
                                            u.subway as u_subway, \
                                            u.call_times as u_call_times, \
                                            u.dx_latest_time as u_dx_latest_time, \
                                            u.dx_tag_max as u_dx_tag_max, \
                                            u.dx_roomcode_list as u_dx_roomcode_list, \
                                            u.daikan_times as u_daikan_times, \
                                            u.dk_saleid_list as u_dk_saleid_list, \
                                            u.dk_latest_saleid as u_dk_latest_saleid, \
                                            u.dk_latest_suiteid as u_dk_latest_suiteid, \
                                            u.daikan_latest_time as u_daikan_latest_time, \
                                            u.rent_willingness as u_rent_willingness, \
                                            u.app_rooms as u_app_rooms, \
                                            u.app_click_rooms as u_app_click_rooms, \
                                            u.dk_greylist as u_dk_greylist, \
                                            u.created_at as u_created_at, \
                                            u.latest_date_edit as u_latest_date_edit, \
                                            r.room_id as r_room_id, \
                                            r.room_code as r_room_code, \
                                            r.suite_id as r_suite_id, \
                                            r.xiaoqu_id as r_xiaoqu_id, \
                                            r.xiaoqu_name as r_xiaoqu_name, \
                                            r.block_id as r_block_id, \
                                            r.block as r_block, \
                                            r.city_name as r_city_name, \
                                            r.room_area as r_room_area, \
                                            r.face as r_face, \
                                            r.has_bathroom as r_has_bathroom, \
                                            r.has_balcony as r_has_balcony, \
                                            r.suite_status as r_suite_status, \
                                            r.bedroom_num as r_bedroom_num, \
                                            r.price as r_price, \
                                            r.has_month_rent as r_has_month_rent, \
                                            r.month_price as r_month_price, \
                                            r.ready_for_rent_date as r_ready_for_rent_date, \
                                            r.sf_remain_days as r_sf_remain_days, \
                                            r.has_lift as r_has_lift, \
                                            r.subway_line as r_subway_line, \
                                            r.subway_name as r_subway_name, \
                                            r.subway_distance as r_subway_distance, \
                                            r.rent_type as r_rent_type, \
                                            u.contract_blacklist as u_contract_blacklist, \
                                            u.dk_blacklist as u_dk_blacklist, \
                                            COALESCE(s.level,0) as level, \
                                            split(translate(contract_blacklist, '|', '\t'),'\t') as u_contract_blacklist_2_list, \
                                            split(translate(dk_blacklist, '|', '\t'),'\t') as u_dk_blacklist_2_list, \
                                            case when r.sf_remain_days <=365 and (u.plan_months < 12 or u.plan_months is null) then 1 else 0 end qualify_less_than_a_year, \
                                            case when r.sf_remain_days > 365 and (u.plan_months >=12 or u.plan_months is null) then 1 else 0 end qualify_more_than_a_year \
                                            from \
                                            (select * from rooms) r join \
                                            (select * from subway_link_info_union) s on r.city_name=s.city and r.block=s.name join \
                                            (select * from effective_users) u on s.neibor_poi_name_0=u.poiblock1 and s.city=u.city")
    df_rooms_users.createOrReplaceTempView("rooms_users")
    # 基本需求过滤
    df_callback = spark_helper.read_from_hive("select *\
                                            from rooms_users \
                                            where abs(r_price-u_price)<=1000 \
                                            and r_rent_type=u_rent_type \
                                            and (u_contract_blacklist is null or array_contains(u_contract_blacklist_2_list, r_room_id) = false)\
                                            and (u_dk_blacklist is null or array_contains(u_dk_blacklist_2_list, r_suite_id) = false)\
                                            and ((qualify_less_than_a_year=1) or (qualify_more_than_a_year=1))")
    df_callback = df_callback.dropDuplicates(subset=["u_mobile", "r_room_id"])
    df_callback.createOrReplaceTempView("reach_user_callback")
    # 重要因素排序打分score1
    df_score1 = spark_helper.read_from_hive("select *, \
                                            case when r_sf_remain_days <= 365 then \
                                            (50 - abs(u_price - r_month_price) * 0.05) + (50 - level * 10) \
                                            else \
                                            (50 - abs(u_price - r_price) * 0.05) + (50 - level * 10) \
                                            end as score1, \
                                            case when r_has_bathroom=1 and u_has_toilet='是' then 30 \
                                            when r_has_bathroom=0 and u_has_toilet='是' then 0 \
                                            else 15 end as score_toilet, \
                                            case when r_subway_line=u_subway then 15 \
                                            else 0 end as score_subway_line, \
                                            case when r_subway_distance<=1000 and u_near_subway='是' then 30 \
                                            when (r_subway_distance >1000 or r_subway_distance is null) and u_near_subway='是' then 0 \
                                            else 13 end as score_near_subway, \
                                            case when r_face=u_face then 15 \
                                            else 0 end as score_face, \
                                            case when u_area=0 then 5 \
                                            when abs(u_area - r_room_area) > 0 and abs(u_area - r_room_area) <= 2 then 10 \
                                            when abs(u_area - r_room_area) > 2 and abs(u_area - r_room_area) <=5 then 5 \
                                            else 0 end as score_area, \
                                            case when r_has_lift=1 and u_has_lift='是' then 10 \
                                            when r_has_lift=0 and u_has_lift='是' then 0 \
                                            else 5 end as score_lift, \
                                            case when r_bedroom_num >= u_room_nums then 10 \
                                            when r_bedroom_num < u_room_nums then 0 \
                                            else 5 end as score_roomnum, \
                                            case when r_has_balcony=1 and u_has_balcony='是' then 10 \
                                            when r_has_balcony=0 and u_has_balcony='是' then 5 \
                                            else 0 end as score_balcony \
                                            from reach_user_callback")
    df_score1.createOrReplaceTempView("score1")
    # 总分计算
    df_scored = spark_helper.read_from_hive(
        "select *, (0.7*score1 + 0.3*(score_toilet + score_subway_line + score_near_subway + score_face + score_area + score_lift + score_roomnum + score_balcony)) as score from score1")
    df_scored.createOrReplaceTempView("scored")
    # 每个客户最多只挂20个房子
    df_scored = spark_helper.read_from_hive("select a.* from \
                                            (select *, row_number() over (partition by u_mobile order by score desc) as num_rank_room \
                                            from scored) a \
                                            where a.num_rank_room <= 20")
    df_scored.createOrReplaceTempView("scored")
    # 根据分数获取最终排序结果
    df_room_customer_matching = spark_helper.read_from_hive("select a.r_room_id as room_id, concat_ws(',', collect_list(a.pair_mobile_score)) as mobiles, from_unixtime(unix_timestamp()) as created_at \
                                from \
                                (select scored.*, \
                                row_number() over (partition by r_room_id order by score desc) as num, \
                                concat_ws(':', cast(u_mobile as string), cast(score as string)) pair_mobile_score \
                                from scored) a \
                                where a.num <= 200 \
                                group by a.r_room_id")
    # 删除room_customer_matching_spark当天的旧数据
    sql_r_u = "delete from room_customer_matching_spark where substring(created_at,1,10) = current_date"
    result = mysql_helper.execute(sql_r_u)
    # 写入room_customer_matching_spark
    time_start_write_room_customer = time.time()
    spark_helper.write_to_mysql_DMADB(df_room_customer_matching, "room_customer_matching_spark")
    time_end_write_room_customer = time.time()
    print('room_customer cost', time_end_write_room_customer - time_start_write_room_customer)
    mysql_helper.close()
    time_end = time.time()
    print('totally cost', time_end - time_start_all)
