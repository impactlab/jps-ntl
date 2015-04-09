from django.shortcuts import render
from django.contrib.auth.views import login
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, RequestContext
from django.shortcuts import get_object_or_404, render, render_to_response
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.core.urlresolvers import reverse
from django.views.generic.edit import UpdateView, CreateView, DeleteView
from django.template import RequestContext
from django.forms.models import modelform_factory
from django_tables2 import RequestConfig
from django import forms
from viewer.forms import GroupFilterForm
import json
from models import *

from viewer import models

class LoginRequiredMixin(object):
  @classmethod
  def as_view(cls, **initkwargs):
    view = super(LoginRequiredMixin, cls).as_view(**initkwargs)
    return login_required(view)

def home(request):
  t = loader.get_template('viewer/home.html')
  context = {}

  formset, queryset = None, None
  if request.GET:
    formset = GroupFilterForm(request.GET)
    if formset.is_valid():
      groups = formset.cleaned_data['groups']
      queryset = Meter.objects.filter(groups__in=groups).all()
    else:
      queryset = Meter.objects.filter(total_usage__gt=0)
  else:
    formset = GroupFilterForm()
    queryset = Meter.objects.all()

  table = MeterTable(queryset)
  RequestConfig(request).configure(table)
  context['form'] = formset
  context['table'] = table
  c = RequestContext(request,context)
  if (request.user.is_authenticated()):
    return HttpResponse(t.render(c))
  return login(request, template_name='viewer/home.html')

@login_required
def profile(request):
  return render(request, template_name='viewer/profile.html')

@login_required
def meter_detail(request, id):
  meter = Meter.objects.get(id=id)
  context = {'meter': meter,
             'ami_heatmap_data': meter.format_ami_data(fmt='json-grid'),
             'recent_preview_data': meter.format_ami_data(fmt='json'),
             'events_data': meter.events_data(),
             'meas_diag_data': meter.meas_diag_data()}
  return render(request, 'viewer/meter_detail.html', context)

@login_required
def auditlist(request):
  if request.method == 'POST':
    pks = [int(i) for i in request.POST.getlist('audit')]
    for meter in Meter.objects.all():
      meter.on_auditlist=True if meter.pk in pks else False
      meter.save()
  return HttpResponseRedirect(reverse('home'))

@login_required
def download_auditlist(request):
  print "dl"
  meters = Meter.objects.filter(on_auditlist=True)
  response = HttpResponse(content_type='text/csv')
  response['Content-Disposition'] = 'attachment; filename="AuditList.csv"'
  f = csv.writer(response)
  for m in meters:
    f.writerow([m.meter_id, m.overall_score])
  return response

@login_required
def download_meter(request, id):
  m = Meter.objects.get(id=id)
  response = HttpResponse(content_type='text/csv')
  response['Content-Disposition'] = 'attachment; filename="'+m.meter_id+'.csv"'
  f = csv.writer(response)
  f.writerow(['Profile'])
  f.writerow(['time','kw','kva'])
  for row in m.format_ami_data(fmt='csv'):
    f.writerow(row)
  f.writerow([''])
  f.writerow(['Event'])
  f.writerow(['time','event'])
  for row in m.format_event_data(fmt='csv'):
    f.writerow(row)
  f.writerow([''])
  f.writerow(['Measurement and Diagnostic'])
  for k,v in m.meas_diag_data().iteritems():
    f.writerow([k,v])
  return response

