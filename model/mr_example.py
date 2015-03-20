from mrjob.job import MRJob
import numpy as np

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            yield fields[0], float(fields[2])
        except IndexError:
            pass

    def reducer(self, key, values):
        profile = np.array(list(values))
        yield key, np.percentile(profile,50)

if __name__ == '__main__':
    MRWordFrequencyCount.run()