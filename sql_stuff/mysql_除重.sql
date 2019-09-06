-- 在没有主键的xiaoqurelaitons中根据新建主键ranking 和 几个字段进行除重
select count(*) from (select 
id,jingdui,jingdui_type,jingdui_district,count(*) num
from xiaoqu.distinct_xiaoqu 
group by id,jingdui,jingdui_type,jingdui_district
-- having count(*)>1
order by num DESC)a


select count(*) from xiaoqu.distinct_xiaoqu


-- delete from xiaoqu.distinct_xiaoqu 
-- where id,jingdui,jingdui_type,jingdui_district in 

-- create table xiaoqu.distinct_xiaoqu as 
-- select * from xiaoqu.xiaoqurelations

-- select count(*) from xiaoqu.distinct_xiaoqu
-- select count(*) from xiaoqu.xiaoqurelations

select id,jingdui,jingdui_type,jingdui_district from xiaoqu.distinct_xiaoqu 
where (id,jingdui,jingdui_type,jingdui_district) in 
	(select 
	id,jingdui,jingdui_type,jingdui_district
	from xiaoqu.distinct_xiaoqu 
	group by id,jingdui,jingdui_type,jingdui_district
	having count(*)>1
	)
order by id,jingdui,jingdui_type,jingdui_district

alter table xiaoqu.distinct_xiaoqu add COLUMN ranking  INT4 PRIMARY key AUTO_INCREMENT

-- select id,city,district,block,type,company,jingdui,note,jingdui_type,jingdui_district from xiaoqu.distinct_xiaoqu 
select * from xiaoqu.distinct_xiaoqu
where (id,jingdui,jingdui_type,jingdui_district) in 
	(select 
	id,jingdui,jingdui_type,jingdui_district
	from xiaoqu.distinct_xiaoqu 
	group by id,jingdui,jingdui_type,jingdui_district
	having count(*)>1
	)
order by id,jingdui,jingdui_type,jingdui_district

-- 471424
select 
min(ranking)
from xiaoqu.distinct_xiaoqu 
group by id,jingdui,jingdui_type,jingdui_district
having count(*)>=1
limit 100

select count(*) from
(
select * from xiaoqu.distinct_xiaoqu
where ranking not in (
	select 
	min(ranking)
	from xiaoqu.distinct_xiaoqu 
	group by id,jingdui,jingdui_type,jingdui_district
	)
)a

-- 最终的语句，上面的都是铺垫
delete from xiaoqu.distinct_xiaoqu
where ranking not in (
	select col from (
		select 
		min(ranking) col
		from xiaoqu.distinct_xiaoqu 
		group by id,jingdui,jingdui_type,jingdui_district
		)a
	)
	
	
select count(*) from xiaoqu.distinct_xiaoqu

create table xiaoqu.xiaoqurelations as 
select id,city,district,block,type,company,jingdui,note,jingdui_type,jingdui_district
from xiaoqu.distinct_xiaoqu

select count(*) from xiaoqu.xiaoqurelations


select if('asdf'='asdf','true','false')