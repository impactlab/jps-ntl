from django.conf.urls import patterns, url
from viewer import views

urlpatterns = patterns('',
        url(r'^$', views.home, name='home'),
        url(r'^meter/(?P<id>\d+)/$', views.meter_detail, name='meter_detail'),
        )
