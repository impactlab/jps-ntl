infile = '../data/features-4-1.txt'
outfile = '../data/features-4-1-cleaned.txt'

import json
import csv

def flatten(container):
    for i in container:
        if isinstance(i, list) or isinstance(i, tuple):
            for j in flatten(i):
                yield j
        else:
            yield i

with open(infile, 'rb') as infh:
    with open(outfile, 'wb') as outfh:
        out_writer = csv.writer(outfh, delimiter=',')
        for line in infh: 
            fields = line.strip().split('\t')
            line_obj = json.loads(fields[1])
            key_obj = json.loads(fields[0])
            if len(line_obj)>1:
                out_writer.writerow(key_obj + list(flatten(line_obj[1:])))      