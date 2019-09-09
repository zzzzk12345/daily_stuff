drop table if exists danke_bi.community_anjuke

DROP TABLE if exists danke_bi.community_beike;

drop table if exists danke_bi.community_xitai


-- beike
CREATE EXTERNAL TABLE `danke_bi.community_beike`(
  `id` bigint COMMENT '自增id', 
  `dk_id` bigint COMMENT '蛋壳ID', 
  `url` string COMMENT '房源信息链接', 
  `bk_id` bigint COMMENT '贝壳ID', 
  `city` string COMMENT '城市', 
  `district` string COMMENT '城区', 
  `business_district` string COMMENT '商圈', 
  `community_name` string COMMENT '小区名', 
  `longitude` double COMMENT '经度', 
  `latitude` double COMMENT '纬度', 
  `sale_price` string COMMENT '平均售价', 
  `saling_num` bigint COMMENT '在售套数', 
  `deal_history` bigint COMMENT '历史成交', 
  `renting_num` bigint COMMENT '在租套数', 
  `bk_score` double COMMENT '贝壳评分', 
  `build_years` string COMMENT '建筑年代,单位:年', 
  `use` string COMMENT '用途', 
  `transactions_ownership` string COMMENT '交易权属', 
  `build_type` string COMMENT '楼型', 
  `property_rights_years` string COMMENT '产权年限,单位:年', 
  `developer` string COMMENT '开发商', 
  `heating_mode` string COMMENT '供暖模式', 
  `heating_fee` string COMMENT '供暖费用,单位:元/㎡', 
  `water_type` string COMMENT '用水类型', 
  `electricity_type` string COMMENT '用电类型', 
  `parking_num` bigint COMMENT '固定车位数', 
  `parking_fee` string COMMENT '停车费用', 
  `gas_fee` string COMMENT '燃气费,单位:元/m³', 
  `plot_ratio` double COMMENT '容积率', 
  `greening_rate` double COMMENT '绿化率', 
  `property_cost` string COMMENT '物业费,单位:元/月/㎡', 
  `property_company` string COMMENT '物业公司', 
  `property_tel` string COMMENT '物业电话', 
  `subway` string COMMENT '地铁', 
  `trip` string COMMENT '出行', 
  dt date comment '时间')
COMMENT '贝壳数据'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
location '/user/zhangkai10/community_beike'
tblproperties("skip.header.line.count"="1")
  
load data inpath 
'/user/zhangkai10/community_beike.csv'
overwrite into table danke_bi.community_beike;

select * from danke_bi.community_beike;

alter table danke_bi.community_beike change column bk_id bk_id bigint

alter table danke_bi.community_beike change column dt dt date
  
  -- anjuke
  CREATE EXTERNAL TABLE `danke_bi.community_anjuke`(
  `id` bigint COMMENT '自增id', 
  dk_id bigint comment '蛋壳id',
  `name` string COMMENT '小区名', 
  `city` string COMMENT '城市', 
  `district` string COMMENT '行政区', 
  `block` string COMMENT '商圈', 
  `longitude` double COMMENT '经度【百度坐标】', 
  `latitude` double COMMENT '纬度【百度坐标】', 
  `open_status` int COMMENT '开区状态，0为未开区，1为已开区', 
  `url` string COMMENT '该小区的安居客url,数据源:安居客小区', 
  `address` string COMMENT '小区地址,数据源:安居客小区', 
  `build_years` string COMMENT '建筑年代,单位:年,数据源:安居客小区', 
  `ajk_id` double COMMENT '安居客id,数据源:安居客小区', 
  `developer` string COMMENT '开发商,数据源:安居客小区', 
  `greening_rate` string COMMENT '绿化率,数据源:安居客小区', 
  `parking_num` string COMMENT '停车位数,数据源:安居客小区', 
  `plot_ratio` string COMMENT '容积率,数据源:安居客小区', 
  `property_cost` string COMMENT '物业费,单位:元/平米/月,数据源:安居客小区', 
  `sale_price` double COMMENT '均价,数据源:安居客小区', 
  `use` string COMMENT '房屋用途（物业类型）,数据源:安居客小区', 
  `houses_num` string COMMENT '总户数,单位:户,数据源:安居客小区', 
  `groundArea` string COMMENT '总建筑面积,单位:m²,数据源:安居客小区', 
  `service_manager` string COMMENT '物业公司,数据源:安居客小区', 
  `use_2` string COMMENT '房屋用途,数据源:安居客二手房', 
  `total_floor` double COMMENT '楼层总数,数据源:安居客二手房/租房', 
  `elevator` string COMMENT '电梯,数据源:安居客二手房', 
  `property_rights_years` string COMMENT '产权年限,单位:年,数据源:安居客二手房/租房', 
  `transactions_ownership` string COMMENT '交易权属,数据源:安居客二手房/租房', 
  `business_district` string COMMENT '商圈,数据源:安居客小区',
  dt date comment '时间'
  )
COMMENT '安居客数据'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
location '/user/zhangkai10/community_anjuke'
tblproperties("skip.header.line.count"="1");

  
load data inpath 
'/user/zhangkai10/community_anjuke.csv'
overwrite into table danke_bi.community_anjuke;

select * from danke_bi.community_anjuke
  
  -- xitai
  CREATE EXTERNAL TABLE `danke_bi.community_xitai`(
  `id` bigint COMMENT '自增id', 
  dk_id bigint comment '蛋壳id',
  `name` string COMMENT '小区名', 
  `city` string COMMENT '城市', 
  `district` string COMMENT '行政区', 
  `block` string COMMENT '商圈', 
  `longitude` double COMMENT '经度【百度坐标】', 
  `latitude` double COMMENT '纬度【百度坐标】', 
  `open_status` bigint COMMENT '开区状态, 0为未开区，1为已开区', 
  `bd_district` string COMMENT '修正后所属行政区', 
  `city_code` bigint COMMENT '城市国标码', 
  `district_code` bigint COMMENT '行政区国标码', 
  `cityName` string COMMENT '城市,数据源:禧泰', 
  `haName` string COMMENT '小区名,数据源:禧泰', 
  `distName` string COMMENT '行政区,数据源:禧泰', 
  `salePriceTime` string COMMENT '出售价格时间(上个月),数据源:禧泰', 
  `salePrice` bigint COMMENT '出售价格(上个月),数据源:禧泰', 
  `leasePriceTime` string COMMENT '出租价格时间(上个月),数据源:禧泰', 
  `leasePrice` double COMMENT '出租价格(上个月),数据源:禧泰', 
  `wgs84gps` double COMMENT '坐标【原始坐标wgs84】,数据源:禧泰', 
  `bd09gps` double COMMENT '坐标【百度坐标】,数据源:禧泰', 
  `propType` string COMMENT '用途,数据源:禧泰', 
  `propertyCost` double COMMENT '物业费,单位:元/㎡/月,数据源:禧泰', 
  `buildYear` string COMMENT '建筑年代,数据源:禧泰', 
  `landUse` string COMMENT '土地类型（产权类型）,数据源:禧泰', 
  `bldgType` string COMMENT '建筑类型,数据源:禧泰', 
  `obLevel` string COMMENT '写字楼级别,数据源:禧泰', 
  `obAutomation` string COMMENT '自动化程度,数据源:禧泰', 
  `groundArea` string COMMENT '占地面积,单位:㎡,数据源:禧泰', 
  `constArea` string COMMENT '建筑面积,单位:㎡,数据源:禧泰',
  dt date comment '时间')
COMMENT '禧泰数据'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
location '/user/zhangkai10/community_xitai'
tblproperties("skip.header.line.count"="1");
  
load data inpath 
'/user/zhangkai10/community_xitai.csv'
overwrite into table danke_bi.community_xitai;

select * from danke_bi.community_xitai

alter table danke_bi.community_xitai change column wgs84gps wgs84gps string 

alter table danke_bi.community_xitai change column bd09gps bd09gps string 