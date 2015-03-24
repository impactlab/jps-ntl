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
            timestamp = time.mktime(meas_datetime.timetuple())
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
        
        pct = np.percentile(readings,50)
        yield key, pct

if __name__ == '__main__':
    ProfilePercentiles.run()