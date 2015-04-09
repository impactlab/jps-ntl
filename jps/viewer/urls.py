from django.conf.urls import patterns, url
from viewer import views

urlpatterns = patterns('',
        url(r'^$', views.home, name='home'),
        url(r'^meter/(?P<id>\d+)/download$', views.download_meter, name='download_meter'),
        url(r'^meter/(?P<id>\d+)/$', views.meter_detail, name='meter_detail'),
        url(r'^auditlist/download$', views.download_auditlist, name='download_auditlist'),
        url(r'^auditlist/$', views.auditlist, name='auditlist'),
        )
