-- hive建表示例代码

CREATE EXTERNAL TABLE `danke_bi.community_beike`(
  `id` bigint COMMENT '自增id', 
  `dk_id` bigint COMMENT '蛋壳ID', 
  `url` string COMMENT '房源信息链接', 
  `bk_id` bigint COMMENT '贝壳ID', 
  `city` string COMMENT '城市', 
  `district` string COMMENT '城区', 
  `business_district` string COMMENT '商圈', 
  `community_name` string COMMENT '小区名', 
  `longitude` string COMMENT '经度', 
  `latitude` string COMMENT '纬度', 
  `sale_price` string COMMENT '平均售价', 
  `saling_num` bigint COMMENT '在售套数', 
  `deal_history` bigint COMMENT '历史成交', 
  `renting_num` bigint COMMENT '在租套数', 
  `bk_score` string COMMENT '贝壳评分', 
  `build_years` string COMMENT '建筑年代', 
  `use` string COMMENT '用途', 
  `transactions_ownership` string COMMENT '交易权属', 
  `build_type` string COMMENT '楼型', 
  `property_rights_years` string COMMENT '产权年限', 
  `developer` string COMMENT '开发商', 
  `heating_mode` string COMMENT '供暖模式', 
  `heating_fee` string COMMENT '供暖费用', 
  `water_type` string COMMENT '用水类型', 
  `electricity_type` string COMMENT '用电类型', 
  `parking_num` bigint COMMENT '固定车位数', 
  `parking_fee` string COMMENT '停车费用', 
  `gas_fee` string COMMENT '燃气费', 
  `plot_ratio` string COMMENT '容积率', 
  `greening_rate` string COMMENT '绿化率', 
  `property_cost` string COMMENT '物业费', 
  `property_company` string COMMENT '物业公司', 
  `property_tel` string COMMENT '物业电话', 
  `subway` string COMMENT '地铁', 
  `trip` string COMMENT '出行', 
  `dt` string COMMENT '时间')
COMMENT '贝壳数据'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
location '/user/zhangkai10/community_beike'
tblproperties("skip.header.line.count"="1");

load data local inpath 
'/user/zhangkai10/community_beike_result.csv'
overwrite into table danke_bi.community_beike;

select * from danke_bi.community_beike;

ALTER TABLE danke_bi.community_beike SET SERDEPROPERTIES ('serialization.encoding'='UTF-8'); 

drop table danke_bi.community_beike