import pandas as pd 
import datetime, time

meter_to_premise_file = r'../data/meter_map_cleaned.csv'
premise_to_flag_file = r'../data/premise_to_flag.csv'
meter_list_file = r'../data/meter_list.csv'

out_file = r'../data/meter_to_flag.csv'
out_investdate_file = r'../data/invest_dates.csv'

meter_list = []
with open(meter_list_file,'rb') as infile:
    lines = infile.readlines()
    for line in lines:
        meter_list = meter_list + line.split(',')

meter_to_premise = pd.read_csv(meter_to_premise_file)
premise_to_flag = pd.read_csv(premise_to_flag_file)

meter_to_flag=pd.merge(premise_to_flag,meter_to_premise,how='inner',left_on='premiseid', right_on='premise_id')

df = meter_to_flag[meter_to_flag['meter_id'].apply(str).isin(meter_list)]

df.to_csv(out_file)

inv_old_name = df.columns[0]
df.rename(columns={inv_old_name:'investid'}, inplace=True)

df['meter_id'] = df['meter_id'].apply(str)
df['investdate'].fillna("2015-04-01 00:00:00", inplace=True)
df['invest_timestamp']=df['investdate'].apply(lambda x: time.mktime(datetime.datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").timetuple()) - 978278400.0)
df[['investid', 'meter_id', 'invest_timestamp']].to_csv(out_investdate_file)


