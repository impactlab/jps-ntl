from django.shortcuts import render
from django.contrib.auth.views import login
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, render_to_response
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.core.urlresolvers import reverse
from django.views.generic.edit import UpdateView, CreateView, DeleteView
from django.template import RequestContext
from django.forms.models import modelform_factory
from django import forms
import json
from models import *

from viewer import models

class LoginRequiredMixin(object):
  @classmethod
  def as_view(cls, **initkwargs):
    view = super(LoginRequiredMixin, cls).as_view(**initkwargs)
    return login_required(view)

def home(request):
  if (request.user.is_authenticated()):
    return render(request, 'viewer/home.html')
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
