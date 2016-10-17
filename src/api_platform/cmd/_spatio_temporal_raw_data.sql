create table if not exists nanjing1_spatio_temporal_raw_data
  (uuid string, lon double, lat double, time bigint) 
  partitioned by (date_p string);

insert overwrite table nanjing1_spatio_temporal_raw_data partition(date_p)
select 
  uuid, lon, lat, time,
  to_char(from_unixtime(time), 'yyyymmdd') as date_p 
  from nanjing1_customer_raw_data 
  where lat is not null and lon is not null and uuid is not null and time is not null 
  and lat>0 and lon>0 and time>0
  group by uuid, time, lat, lon
