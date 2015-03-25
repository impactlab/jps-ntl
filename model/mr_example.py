from mrjob.job import MRJob
import numpy as np
import datetime, time

"""
USAGE:

to run locally: 
python mr_example.py <filename> 

to run on EMR: 
-Set the instance count and other parameters in mrjob.conf , then copy to /etc/
-Set envoroment variables $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY
-then the command to test on one file is is: 
python mr_example.py -r emr s3://jps-ami-dumps/1096444__2001-01-01T000000__2015-01-01T000000.csv 
-to run on the whole directory run: 
python mr_example.py -r emr s3://jps-ami-dumps/
"""

class ProfilePercentiles(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            meas_datetime = datetime.datetime.strptime(fields[1], "%Y/%m/%d %H:%M")
            #Seconds since 2001/01/01
            timestamp = time.mktime(meas_datetime.timetuple()) - 978278400.0
            yield fields[0], [float(timestamp), float(fields[2])]
        except IndexError:
            pass

    def reducer(self, key, values):
        readings = []
        timestamps = []
        for val in values:
            readings.append(val[1])
            timestamps.append(val[0])
        readings = np.asarray(readings)
        timestamps = np.asarray(timestamps)

        perm = timestamps.argsort()
        
        readings = readings[perm]
        timestamps = timestamps[perm]
        
        n_readings = len(readings)
        
        #percentiles
        pct = np.percentile(readings,[0, 0.5, 1, 5, 10, 25, 50, 75, 90, 100])
        
        #fraction below threshhold 
        frac01 = float(sum(readings<0.1))/len(readings)
        frac1 = float(sum(readings<1.0))/len(readings)
        low_counts = np.bincount(((timestamps - timestamps[0])%86400).astype('int64'),weights=(readings<0.1))
        low_pct = np.percentile(low_counts, [50, 90, 95, 99, 100])
        low_fracs = [frac01, frac1, list(low_pct)]       
        
        #Time breakdowns
        weekday_median=np.median(readings[timestamps%604800>432000])
        weekend_median=np.median(readings[timestamps%604800<432000])
        day_median=np.median(readings[(timestamps-32400)%86400>43200])
        night_median=np.median(readings[(timestamps-32400)%86400<43200])
        time_breaks = [weekday_median, weekend_median, day_median, night_median]
        
        #Sliding windows 
        edge_window = lambda ll: np.concatenate((np.ones(ll)*1.0, np.ones(ll)*-1.0))
        window_week = np.max(np.convolve(readings, edge_window(672),'valid'))
        window_month = np.max(np.convolve(readings, edge_window(2688),'valid'))
        windows = [window_week, window_month]
        
        
        #High frequency power spectrum
        ps = np.abs(np.fft.fft(readings))
        hfps = sum(ps[int(n_readings/4):int(n_readings/2)])/sum(ps)    
        
        yield (key, [list(pct), low_fracs, time_breaks, windows, hfps])

if __name__ == '__main__':
    ProfilePercentiles.run()