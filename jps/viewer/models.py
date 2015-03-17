from django.db import models
from django.conf import settings
from django.contrib.auth.models import User, Group
from django.core.urlresolvers import reverse
from django.contrib.contenttypes import generic
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.signals import request_finished
import string, os, fnmatch, csv, datetime, pytz, json, math
import pandas as pd
import numpy as np

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

class EventDataPoint(models.Model):
  meter = models.ForeignKey('Meter', related_name='events')
  ts = models.DateTimeField()
  event = models.CharField(max_length=200)

class ProfileDataPoint(models.Model):
  meter = models.ForeignKey('Meter', related_name='profile_points')
  ts = models.DateTimeField()
  kwh = models.FloatField()
  kva = models.FloatField() 

class MeasurementDataPoint(models.Model):
  meter = models.ForeignKey('Meter', related_name='measurement_points')
  ts = models.DateTimeField()
  time_of_last_interrogation = models.DateTimeField(null=True)
  time_of_last_outage = models.DateTimeField(null=True)

  phase_a_voltage = models.FloatField()
  phase_a_current = models.FloatField()
  phase_a_current_angle = models.FloatField()
  phase_a_dc_detect = models.FloatField()
  phase_b_voltage = models.FloatField()
  phase_b_voltage_angle = models.FloatField()
  phase_b_current = models.FloatField()
  phase_b_current_angle = models.FloatField()
  phase_b_dc_detect = models.FloatField()
  phase_c_voltage = models.FloatField()
  phase_c_voltage_angle = models.FloatField()
  phase_c_current = models.FloatField()
  phase_c_current_angle = models.FloatField()
  phase_c_dc_detect = models.FloatField()

  abc_phase_rotation = models.IntegerField()

  daylight_savings_time_configured = models.NullBooleanField()
  low_battery_error = models.NullBooleanField()
  metrology_communications_fatal_error = models.NullBooleanField()
  inactive_phase = models.NullBooleanField()
  file_system_fatal_error = models.NullBooleanField()
  voltage_deviation = models.NullBooleanField()
  phase_angle_displacement = models.NullBooleanField()
  slc_error = models.NullBooleanField()
  tou_schedule_error = models.NullBooleanField()
  reverse_power_flow_error = models.NullBooleanField()
  register_full_scale_exceeded_error = models.NullBooleanField()
  epf_data_fatal_error = models.NullBooleanField()
  demand_threshold_exceeded_error = models.NullBooleanField()
  metrology_communications_error = models.NullBooleanField()
  ram_fatal_error = models.NullBooleanField()
  phase_loss_error = models.NullBooleanField()
  mass_memory_error = models.NullBooleanField()
  cross_phase_flow = models.NullBooleanField()
  current_waveform_distorsion = models.NullBooleanField()
  mcu_flash_fatal_error = models.NullBooleanField()
  data_flash_fatal_error = models.NullBooleanField()
  clock_sync_error = models.NullBooleanField()
  site_scan_error = models.NullBooleanField()

  diag_count_2 = models.IntegerField()
  diag_count_3 = models.IntegerField()
  diag_count_4 = models.IntegerField()
  diag_5_phase_b_count = models.IntegerField()
  diag_5_phase_a_count = models.IntegerField()
  diag_5_phase_c_count = models.IntegerField()
  times_programmed_count = models.IntegerField()
  early_power_fail_count = models.IntegerField()
  power_outage_count = models.IntegerField()
  good_battery_reading = models.IntegerField()
  demand_reset_count = models.IntegerField()
  demand_interval_length = models.IntegerField()
  current_battery_reading = models.IntegerField()
  current_season = models.IntegerField()
  days_since_demand_reset = models.IntegerField()
  days_since_last_test = models.IntegerField()
  days_on_battery = models.IntegerField()
  service_type_detected = models.IntegerField()
  diag_count_1 = models.IntegerField()
  diag_count_5 = models.IntegerField()

  def _load_csv_line(self, header, data):
    tz = pytz.timezone('America/Jamaica')
    for h,d in zip(header, data):
      if h=="DeviceId": self.meter = Meter.objects.get(meter_id=d) 
      elif h=="Code": 
        ts = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        ts = ts.replace(tzinfo=tz)
        self.ts = ts
      elif h=="Time Of Last Interrogation": 
        if d=='': continue
        ts = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        ts = ts.replace(tzinfo=tz)
        self.time_of_last_interrogation = ts
      elif h=="Time Of Last Outage": 
        if d=='': continue
        ts = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        ts = ts.replace(tzinfo=tz)
        self.time_of_last_outage = ts
      elif h=="Phase B Current angle": self.phase_b_current_angle = float(d)
      elif h=="Daylight Savings Time Configured": self.daylight_savings_time_configured = True if d=='True' else False
      elif h=="Low Battery Error": self.low_battery_error = True if d=='True' else False
      elif h=="Metrology Communications Fatal Error": self.metrology_communications_fatal_error = True if d=='True' else False
      elif h=="Diagnostic Error (Inactive Phase)": self.inactive_phase = True if d=='True' else False
      elif h=="Diag Count 3": self.diag_count_3 = int(d)
      elif h=="Diag Count 4": self.diag_count_4 = int(d)
      elif h=="Times Programmed Count": self.times_programmed_count = int(d)
      elif h=="Early Power Fail Count": self.early_power_fail_count = int(d)
      elif h=="Good Battery Reading": self.good_battery_reading = int(d)
      elif h=="File System Fatal Error": self.file_system_fatal_error = True if d=='True' else False
      elif h=="Diag Count 2": self.diag_count_2 = int(d)
      elif h=="Phase A Current": self.phase_a_current = float(d)
      elif h=="Phase A Current angle": self.phase_a_current_angle = float(d)
      elif h=="ABC PHASE Rotation": self.abc_phase_rotation = int(d)
      elif h=="Diagnostic Error (Voltage Deviation)": self.voltage_deviation = True if d=='True' else False
      elif h=="Diagnostic Error (Phase Angle Displacement)": self.phase_angle_displacement = True if d=='True' else False
      elif h=="Power Outage Count": self.power_outage_count = int(d)
      elif h=="SLC Error": self.slc_error = True if d=='True' else False
      elif h=="Phase B Voltage": self.phase_b_voltage = float(d)
      elif h=="Phase B Voltage angle": self.phase_b_voltage_angle = float(d)
      elif h=="Phase C Voltage angle": self.phase_c_voltage_angle = float(d)
      elif h=="Phase C Current angle": self.phase_c_current_angle = float(d)
      elif h=="Phase A DC Detect": self.phase_a_dc_detect = float(d)
      elif h=="Demand Reset Count": self.demand_reset_count = int(d)
      elif h=="TOU Schedule Error": self.tou_schedule_error = True if d=='True' else False
      elif h=="Reverse Power Flow Error": self.reverse_power_flow_error = True if d=='True' else False
      elif h=="Diag 5 Phase B Count": self.diag_5_phase_b_count = int(d)
      elif h=="Demand Interval Length": self.demand_interval_length = int(d)
      elif h=="Phase B DC Detect": self.phase_b_dc_detect = float(d)
      elif h=="Register Full Scale Exceeded Error": self.register_full_scale_exceeded_error = True if d=='True' else False
      elif h=="EPF Data Fatal Error": self.epf_data_fatal_error = True if d=='True' else False
      elif h=="Phase A Voltage": self.phase_a_voltage = float(d)
      elif h=="Phase B Current": self.phase_b_current = float(d)
      elif h=="Phase C Current": self.phase_c_current = float(d)
      elif h=="Current Battery Reading": self.current_battery_reading = int(d)
      elif h=="Demand Threshold Exceeded Error": self.demand_threshold_exceeded_error = True if d=='True' else False
      elif h=="Metrology Communications Error": self.metrology_communications_error = True if d=='True' else False
      elif h=="RAM Fatal Error": self.ram_fatal_error = True if d=='True' else False
      elif h=="Diag 5 Phase A Count": self.diag_5_phase_a_count = int(d)
      elif h=="Diag 5 Phase C Count": self.diag_5_phase_c_count = int(d)
      elif h=="Current Season": self.current_season = int(d)
      elif h=="Phase C DC Detect": self.phase_c_dc_detect = float(d)
      elif h=="Days Since Demand Reset": self.days_since_demand_reset = int(d)
      elif h=="Days Since Last Test": self.days_since_last_test = int(d)
      elif h=="Days On Battery": self.days_on_battery = int(d)
      elif h=="Phase Loss Error": self.phase_loss_error = True if d=='True' else False
      elif h=="Mass Memory Error": self.mass_memory_error = True if d=='True' else False
      elif h=="Diagnostic Error (Cross Phase Flow)": self.cross_phase_flow = True if d=='True' else False
      elif h=="Diagnostic Error (Current Waveform Distorsion)": self.current_waveform_distorsion = True if d=='True' else False
      elif h=="Phase C Voltage": self.phase_c_voltage = float(d)
      elif h=="Service Type Detected": self.service_type_detected = int(d)
      elif h=="MCU Flash Fatal Error": self.mcu_flash_fatal_error = True if d=='True' else False
      elif h=="Data Flash Fatal Error": self.data_flash_fatal_error = True if d=='True' else False
      elif h=="Diag Count 1": self.diag_count_1 = int(d)
      elif h=="Diag Count 5": self.diag_count_5 = int(d)
      elif h=="Clock Sync Error": self.clock_sync_error = True if d=='True' else False
      elif h=="Site Scan Error": self.site_scan_error = True if d=='True' else False
      else: print 'XXX '+h+' unrecognized'
