from django.db import models
from django.conf import settings
from django.contrib.auth.models import User, Group
from django.core.urlresolvers import reverse
from django.contrib.contenttypes import generic
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.signals import request_finished
import string, os, fnmatch, csv, datetime, pytz, json
import pandas as pd
import numpy as np

class ProfileDataPoint(models.Model):
  meter = models.ForeignKey('Meter', related_name='profile_points')
  ts = models.DateTimeField()
  kwh = models.FloatField()
  kva = models.FloatField() 

class Meter(models.Model):
  meter_id = models.CharField(max_length=32)
  user = models.ForeignKey(User, related_name='meters')

  def __unicode__(self):
    return unicode(self.meter_id)

  def get_absolute_url(self):
    return reverse('detail', kwargs={'id': self.id})

  def _load_data(self, dir='/data/profile'):
    tz = pytz.timezone('America/Jamaica')
    for f in os.listdir(dir):
      if fnmatch.fnmatch(f, self.meter_id+'__*'):
        with open(dir+'/'+f, 'r') as myf:
          fcsv = csv.reader(myf)
          for line in fcsv:
            ts = datetime.datetime.strptime(line[1], '%Y/%m/%d %H:%M')
            ts = ts.replace(tzinfo=tz)
            ProfileDataPoint.objects.create(\
              meter=self, ts=ts, \
              kwh=float(line[2]), kva=float(line[3]))

  def format_ami_data(self, npoints=30*96, fmt='json'):
    if fmt=='json':
      data = [{'date': i.ts.strftime('%Y-%m-%d %H:%M'), 'reading': i.kva}
              for i in reversed(\
                self.profile_points.order_by('-ts')[0:npoints])] 
      return json.dumps(data)
    if fmt=='json-grid':
      raw = [i for i in reversed(self.profile_points.order_by('-ts')\
             [0:npoints])]
      s = pd.Series([i.kva for i in raw], index=[i.ts for i in raw])
      data = [{'date': d.strftime('%Y-%m-%d'), 
               'time': d.strftime('%H:%M'),
               'reading': v} for d,v in \
               s.resample('15T').dropna().iteritems()]
      return json.dumps(data)

  def total_usage(self, start_date=None, end_date=None):
    qs = self.profile_points
    if start_date is not None:
      qs = qs.filter(ts__gte=start_date)
    if end_date is not None:
      qs = qs.filter(ts__lt=end_date)
    return sum([i.kwh for i in qs.all()])

