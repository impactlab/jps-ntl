#!/usr/bin/env python
import numpy as np
import pandas as pd
import datetime, time, csv

class ProfileFeatures:
    def __init__(self, filename):
        ts, kw = [], []
        try:
          with open(filename, 'r') as f:
            fcsv = csv.reader(f)
            for fields in fcsv:
                meas_datetime = \
                  datetime.datetime.strptime(fields[1], "%Y/%m/%d %H:%M")
                reading = float(fields[2])
                ts.append(meas_datetime)
                kw.append(reading)
        except:
          self.features = None
          return

        if len(ts) == 0:
            self.features = None
            return

        self.s = pd.Series(kw, index=ts).resample('15T').dropna()
        self.n = float(self.s.count())
        self.features = []

        # Quantiles
        pct = self.s.quantile(\
          [0.0, .005, .01, .05, .1, .25, .5, .75, .9, 1.0]).values
        self.features.extend(list(pct))
        
        #fraction below threshhold 
        frac01 = self.s[self.s < 0.1].count() / self.n
        frac1 = self.s[self.s < 1.0].count() / self.n
        self.features.extend([frac01, frac1])
        if frac01 > 0.9:
            self.features = None
            return

        #Time breakdowns
        weekday = self.s[self.s.index.dayofweek < 5]
        weekend = self.s[self.s.index.dayofweek >= 5]
        workday = weekday[(weekday.index.hour >= 9) & \
                          (weekday.index.hour < 17)]
        worknight = weekday[(weekday.index.hour < 9) | \
                            (weekday.index.hour >= 17)]
        weekday_median = weekday.median()
        weekend_median = weekend.median()
        workday_median = workday.median()
        worknight_median = worknight.median()
        self.features.extend([weekday_median, weekend_median, \
                              workday_median, worknight_median])
        weekday_mean = weekday.mean()
        weekend_mean = weekend.mean()
        workday_mean = workday.mean()
        worknight_mean = worknight.mean()
        self.features.extend([weekday_mean, weekend_mean, \
                              workday_mean, worknight_mean])

        self.features.extend([weekday_median/weekend_median if weekend_median>0 else 0,\
                              workday_median/worknight_median if worknight_median>0 else 0,\
                              workday_median/weekend_median if weekend_median>0 else 0])
                              

        # Derivatives
        deriv = self.s[1:].values - self.s[:-1].values
        deriv_median = np.median(deriv)
        deriv_mean = np.mean(deriv)
        deriv_abs_median = np.median(np.abs(deriv))
        deriv_abs_mean = np.mean(np.abs(deriv))
        deriv2 = deriv[1:] - deriv[:-1]
        deriv2_median = np.median(deriv2)
        deriv2_mean = np.mean(deriv2)
        deriv2_abs_median = np.median(np.abs(deriv2))
        deriv2_abs_mean = np.mean(np.abs(deriv2))
        self.features.extend([deriv_median, deriv_mean, \
                              deriv_abs_median, deriv_abs_mean, \
                              deriv2_median, deriv2_mean, \
                              deriv2_abs_median, deriv2_abs_mean])
        
        #High frequency power spectrum
        ps = np.abs(np.fft.fft(self.s.values))
        hfps = sum(ps[len(ps)/4:len(ps)/2])/sum(ps[0:len(ps)/2])    
        self.features.extend([hfps])
        
if __name__ == '__main__':
   with open('/data/labels/meter_to_flag.csv', 'r') as f: 
       out = open('features.csv','w')
       outcsv = csv.writer(out)
       fcsv = csv.reader(f)
       fcsv.next()
       header = ['id']
       for line in fcsv:
           meterid = line[-3]
           if line[5]=='': continue
           thisdate = line[5]
           investdate = datetime.datetime.strptime(line[5], '%Y-%m-%d %H:%M:%S')
           start_date = investdate - datetime.timedelta(days=30)
           fname = meterid+'__'+start_date.strftime('%Y-%m-%dT%H%M%S')+'__'+\
                   investdate.strftime('%Y-%m-%dT%H%M%S')+'.csv'
           p = ProfileFeatures('/data/train_profile/'+fname)
           if p.features is not None:
               if len(header)==1:
                   header.extend(['f'+str(i) for i in range(len(p.features))])
                   outcsv.writerow(header)
               outrow = [line[0]]
               outrow.extend(p.features)
               outcsv.writerow(outrow)
   out.close()
