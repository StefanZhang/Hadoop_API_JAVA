# train-v3: Based on moblie carrier data, count each user's total data usage using Mapreduce.
Given access.log, 

col[1] phone number

col[-3] upload data

col[-2] download data

Customize access class:
phone number, upload data, download data, total data

Mapper: Split phone number, upload data, download data, Key:phone#, value:access

Reducer: (phone number, <access, access>)
