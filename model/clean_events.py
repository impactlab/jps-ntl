import json, datetime, time
import pandas as pd
import boto
import os

infile = '../data/events-3-31-2.txt'
outfile = '../data/events-3-31-2-cleaned.txt'

ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
BUCKET_NAME = 'jps-events'

GET_METERS_S3 = True

def get_names_from_bucket(ACCESS_KEY, SECRET_KEY, BUCKET_NAME):
    '''
    This is a nasty kludge to deal with the situation where we have 
    a bunch of event dumps, but they aren't associated with investigation ids. 
    
    Unfortunately if there are no events, there are no actual lines in the .csvs 
    indicating that this meter had its data dumped, but ther were no events. 
    '''
    
    extra_data_file = 'invest_dates.csv'
    df_investdate = pd.read_csv(extra_data_file)    
    
    conn = boto.connect_s3(ACCESS_KEY, SECRET_KEY)
    try:
        bucket = conn.get_bucket(BUCKET_NAME, validate = True)
    except boto.exception.S3ResponseError, e:
        do_something() # The bucket does not exist, choose how to deal with it or raise the exception
    
    eventfile_names = [ key.name.encode( "utf-8" ) for key in bucket.list() ]
    
    meter_list = []
    for name in eventfile_names:
        try:
            fields = name.split('__')
            meter_id = fields[1]
            invest_time = datetime.datetime.strptime(fields[3].split('.')[0], '%Y-%m-%dT%H%M%S')
            invest_timestamp = time.mktime(invest_time.timetuple()) - 978278400.0
            
            invest_match = df_investdate[(df_investdate['invest_timestamp']==invest_timestamp) & (df_investdate['meter_id'] == int(meter_id))]
            invest_id = invest_match['investid'].iloc[0]
            meter_list.append((meter_id, invest_id))
        except IndexError:
            pass
    return list(set(meter_list))

with open(infile, 'rb') as infh:
    count_dict = {}
    for line in infh: 
        (kk, counts) = line.split('\t')
        key_obj = json.loads(kk)
        
        meter_id = key_obj[0][0]
        event_type = key_obj[0][1]
        invest_id = key_obj[1]
        
        count_dict[(meter_id, event_type, invest_id)] = counts
    (meter_ids, event_types, invest_ids) = zip(*count_dict.keys())
    
    ids = zip(meter_ids, invest_ids)
    
    unique_events = list(set(event_types))
    if GET_METERS_S3:
        s_ids = set(ids)
        s_s3 = set(get_names_from_bucket(ACCESS_KEY, SECRET_KEY, BUCKET_NAME))
        unique_ids = list(s_ids.union(s_s3))
    else:
        unique_ids = list(set(ids))
      
    df = pd.DataFrame(index=ids, columns=unique_events)
    df = df.fillna(0) # with 0s rather than NaNs    
    
    for k, v in count_dict.items():
        df[k[1]][(k[0], k[2])] = v
        
    df['meter_id'] = [ix[0] for ix in df.index]
    df['invest_id'] = [ix[1] for ix in df.index]                
    
    df.to_csv(outfile)
        