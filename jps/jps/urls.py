from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'jps.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^viewer/', include('viewer.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^accounts/', include('registration.backends.default.urls')),
    url(r'^$', 'viewer.views.home', name='home'),
    url(r'^profile/$', 'viewer.views.profile', name='profile'),
    url(r'^select2/', include('django_select2.urls')),
)
