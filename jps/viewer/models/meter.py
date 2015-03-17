from django.db import models
from django.conf import settings
from django.contrib.auth.models import User, Group
from django.core.urlresolvers import reverse
from django.contrib.contenttypes import generic
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.signals import request_finished
from django.utils.safestring import mark_safe
import string, os, fnmatch, csv, datetime, pytz, json, math
import pandas as pd
import numpy as np

class Meter(models.Model):
  meter_id = models.CharField(max_length=32)
  user = models.ForeignKey(User, related_name='meters')
  overall_score = models.FloatField(default=0.0)
  on_auditlist = models.BooleanField(default=False)

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
            try: 
              dp = ProfileDataPoint.objects.get(\
                meter=self, ts=ts, \
                kwh=float(line[2]), kva=float(line[3]))
            except:
              ProfileDataPoint.objects.create(\
                meter=self, ts=ts, \
                kwh=float(line[2]), kva=float(line[3]))

  def _load_event_data(self, dir='/data/events'):
    tz = pytz.timezone('America/Jamaica')
    for f in os.listdir(dir):
      if fnmatch.fnmatch(f, self.meter_id+'*'):
        with open(dir+'/'+f, 'r') as myf:
          fcsv = csv.reader(myf)
          fcsv.next()
          for line in fcsv:
            ts = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S')
            ts = ts.replace(tzinfo=tz)
            try: 
              dp = EventDataPoint.objects.get(\
                meter=self, ts=ts, \
                event=line[2])
            except:
              EventDataPoint.objects.create(\
                meter=self, ts=ts, \
                event=line[2])

  def meas_diag_data(self, date=None):
    if self.measurement_points.count()==0:
      return {}
    if date is None:
      ts = self.measurement_points.order_by('-ts')[0].ts
    else:  
      ts = self.measurement_points.filter(ts__lt=date).order_by('-ts')[0].ts
    mp = self.measurement_points.get(ts=ts)
    return { 'phase_a_voltage': mp.phase_a_voltage,
             'phase_a_current': mp.phase_a_current,
             'phase_a_current_angle': mp.phase_a_current_angle,
             'phase_b_voltage': mp.phase_b_voltage,
             'phase_b_voltage_angle': mp.phase_b_voltage_angle,
             'phase_b_current': mp.phase_b_current,
             'phase_b_current_angle': mp.phase_b_current_angle,
             'phase_c_voltage': mp.phase_c_voltage,
             'phase_c_voltage_angle': mp.phase_c_voltage_angle,
             'phase_c_current': mp.phase_c_current,
             'phase_c_current_angle': mp.phase_c_current_angle,
             'max_voltage': max([mp.phase_a_voltage, mp.phase_b_voltage,\
                                 mp.phase_c_voltage, 0]),
             'max_current': max([mp.phase_a_current, mp.phase_b_current,\
                                 mp.phase_c_current, 0]),
             'pf_a': math.cos(mp.phase_a_current_angle*math.pi/180.0),
             'pf_b': math.cos((mp.phase_b_voltage_angle-
                               mp.phase_b_current_angle)*math.pi/180.0),
             'pf_c': math.cos((mp.phase_c_voltage_angle-
                               mp.phase_c_current_angle)*math.pi/180.0),
             'cross_phase_flow': str(mp.cross_phase_flow),
             'voltage_deviation': str(mp.voltage_deviation),
             'inactive_phase': str(mp.inactive_phase),
             'phase_angle_displacement': str(mp.phase_angle_displacement),
             'current_waveform_distorsion': str(mp.current_waveform_distorsion),
             'low_battery_error': str(mp.low_battery_error),
             'mass_memory_error': str(mp.mass_memory_error),
             'phase_loss_error': str(mp.phase_loss_error),
             'reverse_power_flow_error': str(mp.reverse_power_flow_error),
             'site_scan_error': str(mp.site_scan_error),
             'tou_schedule_error': str(mp.tou_schedule_error),
    }
    

  def events_data(self, start_date=None):
    if start_date is None:
      tslast = self.profile_points.order_by('-ts')[0].ts
      start_date = tslast - datetime.timedelta(days=30)
    data = [{'date': i.ts.strftime('%Y-%m-%d %H:%M:%S'), 'text': i.event}
              for i in reversed(\
                self.events.filter(ts__gte=start_date).\
                order_by('-ts'))] 
    return json.dumps(data)
           

  def format_ami_data(self, start_date=None, fmt='json'):
    if start_date is None:
      tslast = self.profile_points.order_by('-ts')[0].ts
      start_date = tslast - datetime.timedelta(days=30)
    if fmt=='json':
      data = [{'date': i.ts.strftime('%Y-%m-%d %H:%M'), 'reading': i.kva}
              for i in reversed(\
                self.profile_points.filter(ts__gte=start_date).\
                order_by('-ts'))] 
      return json.dumps(data)
    if fmt=='json-grid':
      raw = [i for i in reversed(self.profile_points.\
             filter(ts__gte=start_date).order_by('-ts'))]
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

import django_tables2 as tables
from django_tables2.utils import A

class MeterTable(tables.Table):
  meter_id = tables.LinkColumn('meter_detail', args=[A('pk')])
  overall_score = tables.Column()
  audit = tables.CheckBoxColumn(accessor="pk", orderable=True,
                                order_by=('-on_auditlist','meter_id'),
    attrs={'th__input': {'type':"text", 'value':"Audit list", 
                         'readonly':None, 'style': 'border: none'}})

  def render_audit(self, record):
    if record.on_auditlist:
      return mark_safe('<input class="auditCheckBox" name="audit" value="'+str(record.pk)+'"" type="checkbox" checked/>')
    else:   
      return mark_safe('<input class="auditCheckBox" name="audit" value="'+str(record.pk)+'"" type="checkbox"/>')
