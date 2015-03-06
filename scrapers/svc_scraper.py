import mechanize
import cookielib
import urllib
import os,time,datetime
from dateutil.relativedelta import relativedelta
from BeautifulSoup import BeautifulSoup

meter_list = ['005T1110']
start_date = datetime.datetime(2011, 1, 1)
stop_date = datetime.datetime(2012,1,1)
incr_date = relativedelta(months=1)

# Parameters
DUMP_SLEEP_TIME = 5.0

svc_domain = r'http://localhost:9090/'

svc_home = svc_domain + 'serview.jsp'
svc_login = svc_domain + 'home_D.jsp'

on_demand_task = svc_domain + 'secure/onDemandTask/onDemandTask_D.jsp'
on_demand_form = svc_domain + 'secure/onDemandTask/onDemandTask_DB.jsp?taskId=link60&TaskManagerID=default'

dump_form_submit = svc_domain + 'secure/onDemandTask/postRequest_F.jsp?chained=&updateDB=true&priority=5&getLog=true&id=link60&requestType=task&TaskManagerID=default&defaultParameter='

user_name = os.environ.get('JPS_USER')
user_pass = os.environ.get('JPS_PASS')

# Browser
br = mechanize.Browser()

# Cookie Jar
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

# Browser options
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

br.addheaders = [('User-agent', r'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko')]
 
# THe site doesn't seem to like it when we try to login directly, if we do it gives
# "Invalid direct reference to form login page"
# So we request the home page first, then try to log in.  
resp = br.open(svc_home)
time.sleep(0.2)

# Now open the login page
resp = br.open(svc_login)

# Select the first form on the login page
br.select_form(nr=0)

# User credentials
br.form['j_username'] = user_name
br.form['j_password'] = user_pass

# Login
resp = br.submit()
time.sleep(0.2)

# On Demand Task
resp = br.open(on_demand_task)
time.sleep(0.2)

# On Deand Form 
resp = br.open(on_demand_form)
time.sleep(0.2)

meter_string = ', '.join(meter_list)

working_begin_date = start_date
while working_begin_date < stop_date:    
    working_end_date = working_begin_date + incr_date
    
    working_begin_string = datetime.datetime.strftime(working_begin_date,'%Y-%m-%d %H:%M:%S')
    working_end_string = datetime.datetime.strftime(working_end_date,'%Y-%m-%d %H:%M:%S')
    
    out_filename = '_'.join(meter_list) + '__' + \
        datetime.datetime.strftime(working_begin_date,'%Y-%m-%dT%H%M%S') + '__' + \
        datetime.datetime.strftime(working_begin_date,'%Y-%m-%dT%H%M%S') + '.csv'
    
    #These are the parameters you've got from checking with the aforementioned tools
    post_fields = {'resourceName': 'link60',
                   'onDemandStartTime.dataType': 'dateTime',
                   'onDemandEndTime.dataType': 'dateTime',
                   'executionMode':'onDemand',
                   'outputFilePath':'/svcdownload/esu_test/',
                   'outputFileName': out_filename,
                   'onDemandType':'list',
                   'onDemandId': meter_string,
                   'onDemandStartTime': working_begin_string,
                   'onDemandEndTime': working_end_string
                 }
    
    
    
    
    #Encode the parameters
    post_data = urllib.urlencode(post_fields)
    #Submit the form (POST request). You get the post_url and the request type(POST/GET) the same way with the parameters.
    resp = br.open(dump_form_submit,post_data)
    time.sleep(DUMP_SLEEP_TIME)
    
    working_begin_date = working_begin_date + incr_date
    


