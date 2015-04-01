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
        self.SECS_IN_DAY = 60*60*24
        
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
            meas_datetime = datetime.datetime.strptime(fields[1], "%Y/%m/%d %H:%M")
            #Seconds since 2001/01/01
            timestamp = time.mktime(meas_datetime.timetuple()) - 978278400.0
            yield fields[0], [float(timestamp), float(fields[2])]
        except (IndexError, ValueError) as e:
            pass

    def reducer(self, key, values):        
        try:
            inspections = self.inspection_dict[key]
        except KeyError:
            inspections = [('no inspection', self.SECS_IN_DAY*365*14.25)]
        
        readings_orig = []
        timestamps_orig = []
        for val in values:
            readings_orig.append(val[1])
            timestamps_orig.append(val[0])
        readings_orig = np.asarray(readings_orig)
        timestamps_orig = np.asarray(timestamps_orig)

        perm = timestamps_orig.argsort()
        
        readings_orig = readings_orig[perm]
        timestamps_orig = timestamps_orig[perm]
        for inspection in inspections:
            inspection_ind = np.searchsorted(timestamps_orig, inspection[1])
            
            readings = readings_orig[:inspection_ind]
            timestamps = timestamps_orig[:inspection_ind]          
              
            features = []
            
            time_ranges = [np.Inf, self.SECS_IN_DAY * 7 * 52, self.SECS_IN_DAY * 7 * 8]
               
            for time_range in time_ranges:
                if len(timestamps)==0:
                    break
                
                start_ix = np.searchsorted(timestamps, timestamps[-1] - time_range)
                
                readings = readings[start_ix:]
                timestamps = timestamps[start_ix:]
                
                n_readings = len(readings)
                if n_readings>0:
                    #percentiles
                    pct_pre = np.percentile(readings,[0, 0.5, 1, 5, 10, 25, 75, 90, 100, 50]) 
                    pct = pct_pre[:-1]/pct_pre[-1]
                    features.extend(pct)
                    
                    #mean
                    read_mean = np.mean(readings)/pct_pre[-1]
                    features.extend([read_mean, n_readings, timestamps[0], pct_pre[-1]])
                    
                    #fraction below threshhold 
                    frac01 = float(sum(readings<0.1))/len(readings)
                    frac1 = float(sum(readings<1.0))/len(readings)
                    low_counts = np.bincount(((timestamps - timestamps[0])/86400).astype('int64'),weights=(readings<0.1))
                    low_pct = np.percentile(low_counts, [50, 90, 95, 99, 100])/96.0
                    low_fracs = [frac01, frac1] + list(low_pct)    
                    features.extend(low_fracs)
                    
                    #Time breakdowns
                    weekday_median=np.median(readings[timestamps%604800>432000])
                    weekend_median=np.median(readings[timestamps%604800<432000])
                    day_median=np.median(readings[(timestamps-32400)%86400>43200])
                    night_median=np.median(readings[(timestamps-32400)%86400<43200])
                    time_breaks = [weekday_median, weekend_median, day_median, night_median]
                    time_breaks = [xx/pct_pre[-1] for xx in time_breaks]
                    features.extend(time_breaks)
                    
                    #Sliding windows 
                    edge_window = lambda ll: np.concatenate((np.ones(ll)*1.0, np.ones(ll)*-1.0))
                    window_week = np.max(np.convolve(readings, edge_window(672),'valid'))
                    window_month = np.max(np.convolve(readings, edge_window(2688),'valid'))
                    windows = [window_week/672, window_month/2888]
                    windows = [xx/pct_pre[-1] for xx in windows]
                    features.extend(windows)
                    
                    # Derivatives
                    deriv = readings[1:]-readings[:-1]
                    deriv_median = np.median(deriv)
                    deriv_mean = np.mean(deriv)
                    deriv_abs_median = np.median(np.abs(deriv))
                    deriv_abs_mean = np.mean(np.abs(deriv))
                    deriv2 = deriv[1:] - deriv[:-1]
                    deriv2_median = np.median(deriv2)
                    deriv2_mean = np.mean(deriv2)
                    deriv2_abs_median = np.median(np.abs(deriv2))
                    deriv2_abs_mean = np.mean(np.abs(deriv2))
                    deriv = ([deriv_median, deriv_mean, \
                                          deriv_abs_median, deriv_abs_mean, \
                                          deriv2_median, deriv2_mean, \
                                          deriv2_abs_median, deriv2_abs_mean])    
                    features.extend(deriv)
                    
                    # High frequency power spectrum
                    ps = np.abs(np.fft.fft(readings))
                    hfps1 = sum(ps[int(n_readings/4):int(n_readings/2)])/sum(ps) 
                    hfps2 = sum(ps[int(n_readings/8):int(n_readings/2)])/sum(ps)  
                    hfps3 = sum(ps[int(n_readings/16):int(n_readings/2)])/sum(ps)  
                    hfps = [hfps1, hfps2, hfps3]
                    features.extend(hfps)     
            yield ((key, inspection[0]), features)
if __name__ == '__main__':
    ProfileFeatures.run()