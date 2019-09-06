-- hive建表实例代码

create external table if not exists danke_bi.community_external_rating (
id bigint comment'自增id',
city string comment'城市',
district string comment'行政区',
block string comment'商圈',
name string comment'小区名',
type string comment'小区类型',
xiaoqu_longitude string comment'小区经度',
xiaoqu_latitude string comment'小区纬度',
subway_id bigint comment'地铁id',
subway_longitude string comment'地铁经度',
subway_latitude string comment'地铁纬度',
subway_distance string comment'距地铁距离',
building_year bigint comment'建筑时间',
houses_num bigint comment'房屋数量',
property_cost string comment'物业成本',
property_cost_unit string comment'物业成本单位',
office_buildings bigint comment'写字楼',
office_buildings_new bigint comment'新写字楼',
elevator bigint comment'电梯数量',
subway_name string comment'地铁名称',
walking_distance bigint comment'徒步距离',
walking_minutes bigint comment'徒步分钟数',
transactions_ownership string comment'交易所有权',
dt string comment'时间'
)
comment'更新后外部评级基础数据'
row format delimited
fields terminated by ','
location '/user/zhangkai10/community_external_rating'
tblproperties("skip.header.line.count"="1");


alter table danke_bi.community_external_rating change column
xiaoqu_longitude xiaoqu_longitude string;

load data inpath 
'/user/zhangkai10/community_external_rating_全国更新_20190903_最终.csv'
overwrite into table danke_bi.community_external_rating;

select * from danke_bi.community_external_rating;

drop table danke_bi.community_external_rating