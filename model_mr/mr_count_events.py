from mrjob.job import MRJob
import numpy as np
import pandas as pd
import datetime, time

"""
USAGE:

to run locally: 
python mr_example.py <filename_or_inputdir> 

to run on EMR: 
-Set the instance count and other parameters in mrjob.conf , then copy to /etc/
-Set envoroment variables $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY
-then the command to test on one file is is: 
python mr_example.py -r emr s3://jps-ami-dumps/1096444__2001-01-01T000000__2015-01-01T000000.csv 
-to run on the whole directory run: 
python mr_example.py -r emr s3://jps-ami-dumps/
"""

class ProfileFeatures(MRJob):
    def reducer_init(self):
        self.SECS_IN_DAY = 60*60*24.
        
        self.inspection_dict = {}
        extra_data_file = 'invest_dates.csv'
        df_investdate = pd.read_csv(extra_data_file)
        for zz in zip([str(mm) for mm in df_investdate['meter_id']], df_investdate['investid'], df_investdate['invest_timestamp']):
            try:
                self.inspection_dict[zz[0]].append((zz[1], zz[2]))
            except KeyError:
                self.inspection_dict[zz[0]]=[(zz[1], zz[2])]    
    
    def mapper(self, _, line):
        fields = [f.strip(' "') for f in line.strip().split(',')]
        try:
            meas_datetime = datetime.datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
            #Seconds since 2001/01/01
            timestamp = time.mktime(meas_datetime.timetuple()) - 978278400.0
            if fields[0]!='Device Id':
                yield ((fields[0], fields[2]), timestamp)
        except (IndexError, ValueError) as e:
            pass

    def reducer(self, key, values):  
        timestamps = list(values)
        
        RECENT_WINDOW = self.SECS_IN_DAY * 30.
        try:
            inspections = self.inspection_dict[key[0]]
        except KeyError:
            inspections = [('no inspection', self.SECS_IN_DAY*365*14.25)]  
            
        for inspection in inspections:
            event_count = 0
            for timestamp in timestamps:
                if ((inspection[1] - timestamp) > 0) and ((inspection[1] - timestamp) < RECENT_WINDOW):
                    event_count = event_count + 1
            yield ((key, inspection[0]), event_count)
        
if __name__ == '__main__':
    ProfileFeatures.run()