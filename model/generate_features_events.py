#!/usr/bin/env python
import numpy as np
import pandas as pd
import datetime, time, csv

class EventFeatures:
    def __init__(self, filename):
        ts, ev = [], []
        try:
          with open(filename, 'r') as f:
            fcsv = csv.reader(f)
            fcsv.next()
            for fields in fcsv:
                if len(fields)==1: continue
                meas_datetime = \
                  datetime.datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
                evt = fields[2]
                ts.append(meas_datetime)
                ev.append(evt)
        except IOError:
          self.features = None
          return

        self.feature_names = ['Event count']
        self.features = [len(ev)]


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
        self.feature_names = []

        # Quantiles
        qs = [0.0, .005, .01, .05, .1, .25, .75, .9, 1.0, 0.5]
        pct = self.s.quantile(qs).values
        if pct[-1]==0: pct[-1]=1e-9
        self.features.extend(list(pct[:-1]/pct[-1]))
        self.feature_names.extend(['quantile%f'%i for i in qs[:-1]])
        
        #fraction below threshhold 
        frac01 = self.s[self.s < 0.1].count() / self.n
        frac1 = self.s[self.s < 1.0].count() / self.n
        self.features.extend([frac01, frac1])
        self.feature_names.extend(['frac01','frac1'])
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
        self.feature_names.extend(['weekday median', 'weekend median', 'workday median', 'worknight median'])
        weekday_mean = weekday.mean()
        weekend_mean = weekend.mean()
        workday_mean = workday.mean()
        worknight_mean = worknight.mean()
        lastyear = self.s[self.s.index < self.s.index[-1] - datetime.timedelta(days=335)]
        thisyear = self.s[self.s.index >= self.s.index[-1] - datetime.timedelta(days=30)]
        meanyoy = lastyear.mean()/thisyear.mean() if thisyear.mean()>0 and not np.isnan(thisyear.mean()) else 0
        medianyoy = lastyear.median()/thisyear.median() if thisyear.median()>0 and not np.isnan(thisyear.median()) else 0
        self.features.extend([weekday_mean, weekend_mean, \
                              workday_mean, worknight_mean, meanyoy, medianyoy])
        self.feature_names.extend(['weekday mean', 'weekend mean', 'workday mean', 'worknight mean', 'meanyoy', 'medianyoy'])

        self.features.extend([weekday_median/weekend_median if weekend_median>0 else 0,\
                              workday_median/worknight_median if worknight_median>0 else 0,\
                              workday_median/weekend_median if weekend_median>0 else 0])
        self.feature_names.extend(['weekday/weekend med', 'workday/worknight med', 'workday/weekend med'])
                              

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
        self.feature_names.extend(['d med', 'd mean', 'd abs med', 'd abs mean', 'd2 med', 'd2 mean', 'd2 abs med', 'd2 abs mean'])
        
        #High frequency power spectrum
        ps = np.abs(np.fft.fft(self.s.values))
        hfps = sum(ps[len(ps)/4:len(ps)/2])/sum(ps)
        hfps2 = sum(ps[len(ps)/8:len(ps)/2])/sum(ps)
        hfps3 = sum(ps[len(ps)/16:len(ps)/2])/sum(ps)
        if np.isfinite(hfps): self.features.extend([hfps])
        else: self.features.extend([0])
        if np.isfinite(hfps2): self.features.extend([hfps2])
        else: self.features.extend([0])
        if np.isfinite(hfps3): self.features.extend([hfps3])
        else: self.features.extend([0])
        self.feature_names.extend(['hfps','hfps2','hfps3'])
        
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
           start_date = investdate - datetime.timedelta(days=365)
           start_date_evt = investdate - datetime.timedelta(days=30)
           fname = meterid+'__'+start_date.strftime('%Y-%m-%dT%H%M%S')+'__'+\
                   investdate.strftime('%Y-%m-%dT%H%M%S')+'.csv'
           p = ProfileFeatures('/data/year_profile_month_evt/'+fname)
           fname_evt = 'evt__'+meterid+'__'+start_date_evt.strftime('%Y-%m-%dT%H%M%S')+'__'+\
                   investdate.strftime('%Y-%m-%dT%H%M%S')+'.csv'
           e = EventFeatures('/data/year_profile_month_evt/'+fname_evt)
           if p.features is not None and e.features is not None:
               print meterid
               features = p.features + e.features
               if len(header)==1:
                   header.extend(p.feature_names)
                   header.extend(e.feature_names)
                   outcsv.writerow(header)
               outrow = [line[0]]
               outrow.extend(features)
               outcsv.writerow(outrow)
   out.close()
