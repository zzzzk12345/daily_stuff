{\rtf1\ansi\ansicpg936\cocoartf1671\cocoasubrtf500
{\fonttbl\f0\fnil\fcharset0 Menlo-Bold;\f1\fnil\fcharset0 Menlo-Regular;\f2\fnil\fcharset134 PingFangSC-Regular;
}
{\colortbl;\red255\green255\blue255;\red128\green0\blue0;\red0\green128\blue0;\red0\green0\blue255;
\red128\green128\blue128;\red0\green0\blue128;}
{\*\expandedcolortbl;;\csgenericrgb\c50196\c0\c0;\csgenericrgb\c0\c50196\c0;\csgenericrgb\c0\c0\c100000;
\csgenericrgb\c50196\c50196\c50196;\csgenericrgb\c0\c0\c50196;}
\paperw11900\paperh16840\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\b\fs24 \cf2 with
\f1\b0 \cf0  base 
\f0\b \cf2 as
\f1\b0 \cf0  (\

\f0\b \cf2 select
\f1\b0 \cf0  \
id,\
data_id,\
table_name,\
diff,\
get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.old'\cf0 ) old_pid,\
get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.new'\cf0 ) new_pid,\

\f0\b \cf2 case
\f1\b0 \cf0  
\f0\b \cf2 when
\f1\b0 \cf0  get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.old'\cf0 ) 
\f0\b \cf2 is
\f1\b0 \cf0  
\f0\b \cf2 not
\f1\b0 \cf0  
\f0\b \cf2 null
\f1\b0 \cf0  \

\f0\b \cf2 then
\f1\b0 \cf0  get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.old'\cf0 ) \

\f0\b \cf2 else
\f1\b0 \cf0  get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.new'\cf0 ) 
\f0\b \cf2 end
\f1\b0 \cf0  pid,\
p_day,\

\f0\b \cf2 case
\f1\b0 \cf0  
\f0\b \cf2 when
\f1\b0 \cf0  get_json_object(get_json_object(a.diff,\cf3 '$.private_id'\cf0 ),\cf3 '$.old'\cf0 ) 
\f0\b \cf2 is
\f1\b0 \cf0  
\f0\b \cf2 null
\f1\b0 \cf0  \

\f0\b \cf2 then
\f1\b0 \cf0  \cf4 0\cf0 \

\f0\b \cf2 else
\f1\b0 \cf0  \cf4 1\cf0  
\f0\b \cf2 end
\f1\b0 \cf0  
\f0\b \cf2 type
\f1\b0 \cf0  \

\f0\b \cf2 from
\f1\b0 \cf0  danke_ods.ods_laputa__model_histories_day_pincr a 
\f0\b \cf2 where
\f1\b0 \cf0  \cf4 1\cf0 =\cf4 1\cf0 \

\f0\b \cf2 and
\f1\b0 \cf0  p_day>=\cf3 '2019-08-01'\cf0 \

\f0\b \cf2 and
\f1\b0 \cf0  table_name=\cf3 'rooms'\cf0 \

\f0\b \cf2 and
\f1\b0 \cf0  get_json_object(a.diff,\cf3 '$.private_id'\cf0 ) 
\f0\b \cf2 is
\f1\b0 \cf0  
\f0\b \cf2 not
\f1\b0 \cf0  
\f0\b \cf2 NULL
\f1\b0 \cf0 \

\f0\b \cf2 order
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  data_id\
),\
\pard\pardeftab720\partightenfactor0
\cf5 ------------\cf0 \
base2 
\f0\b \cf2 as
\f1\b0 \cf0  (\
\pard\pardeftab720\partightenfactor0

\f0\b \cf2 select
\f1\b0 \cf0  id,data_id room_data_id,pid private_id,\
\pard\pardeftab720\partightenfactor0

\f0\b \cf6 floor
\f1\b0 \cf0 ((
\f0\b \cf2 row_number
\f1\b0 \cf0 () 
\f0\b \cf2 over
\f1\b0 \cf0 (
\f0\b \cf2 partition
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  data_id,pid 
\f0\b \cf2 order
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  p_day)+\cf4 1\cf0 )/\cf4 2\cf0 ) 
\f0\b \cf2 as
\f1\b0 \cf0  gp,\cf5 --
\f2 \'d2\'bb\'b4\'ce
\f1  old
\f2 \'a3\'ac\'d2\'bb\'b4\'ce
\f1 new
\f2 \'b7\'d6\'d2\'bb\'b8\'f6\'d7\'e9
\f1 \cf0 \
\pard\pardeftab720\partightenfactor0

\f0\b \cf2 case
\f1\b0 \cf0  
\f0\b \cf2 when
\f1\b0 \cf0  
\f0\b \cf2 type
\f1\b0 \cf0  = \cf4 0\cf0  
\f0\b \cf2 then
\f1\b0 \cf0  p_day 
\f0\b \cf2 else
\f1\b0 \cf0  
\f0\b \cf2 null
\f1\b0 \cf0  
\f0\b \cf2 end
\f1\b0 \cf0  start_date,\

\f0\b \cf2 case
\f1\b0 \cf0  
\f0\b \cf2 when
\f1\b0 \cf0  
\f0\b \cf2 type
\f1\b0 \cf0  = \cf4 1\cf0  
\f0\b \cf2 then
\f1\b0 \cf0  p_day 
\f0\b \cf2 else
\f1\b0 \cf0  
\f0\b \cf2 null
\f1\b0 \cf0  
\f0\b \cf2 end
\f1\b0 \cf0  end_date\

\f0\b \cf2 from
\f1\b0 \cf0  base\

\f0\b \cf2 order
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  room_data_id,private_id,id\
),\
\pard\pardeftab720\partightenfactor0
\cf5 ------------\cf0 \
base3 
\f0\b \cf2 as
\f1\b0 \cf0  (\
\pard\pardeftab720\partightenfactor0

\f0\b \cf2 select
\f1\b0 \cf0  room_data_id,private_id,start_date,\
lead(end_date,\cf4 1\cf0 ) 
\f0\b \cf2 over
\f1\b0 \cf0 (
\f0\b \cf2 partition
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  room_data_id,private_id,gp 
\f0\b \cf2 order
\f1\b0 \cf0  
\f0\b \cf2 by
\f1\b0 \cf0  room_data_id,private_id,id ) end_date\

\f0\b \cf2 from
\f1\b0 \cf0  base2)\
\pard\pardeftab720\partightenfactor0
\cf5 ------------\cf0 \
\pard\pardeftab720\partightenfactor0

\f0\b \cf2 select
\f1\b0 \cf0  room_data_id,private_id,start_date,end_date \

\f0\b \cf2 from
\f1\b0 \cf0  base3\

\f0\b \cf2 where
\f1\b0 \cf0  start_date 
\f0\b \cf2 is
\f1\b0 \cf0  
\f0\b \cf2 not
\f1\b0 \cf0  
\f0\b \cf2 null
\f1\b0 \cf0 \
\
\
}