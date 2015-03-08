import mechanize
import cookielib
import urllib
import os,time,datetime,re
from dateutil.relativedelta import relativedelta
from BeautifulSoup import BeautifulSoup

meter_list = ['005T1110']
start_date = datetime.datetime(2011, 1, 1)
stop_date = datetime.datetime(2012,1,1)
incr_date = relativedelta(months=1)

# Parameters
MAX_METER_IX = 8871 
METER_IX_INCR = 60
OUTFILE_NAME = 'meter_list.csv'

DUMP_SLEEP_TIME = 5.0

svc_domain = r'http://localhost:9090/'

svc_home = svc_domain + 'serview.jsp'
svc_login = svc_domain + 'home_D.jsp'

meter_names = svc_domain + 'secure/selectDevice/selectResult_DT.jsp?deviceIdPattern=%25&execute=search+all+devices&cachedRequestId=_cachedDataSetRequestID_0&currentPosition=60'

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

current_meter_ix = 0
meter_list = []
while current_meter_ix + METER_IX_INCR < MAX_METER_IX:
    meter_names = svc_domain + 'secure/selectDevice/selectResult_DT.jsp?deviceIdPattern=%25&execute=search+all+devices&cachedRequestId=_cachedDataSetRequestID_0&currentPosition=' \
        + str(current_meter_ix)
    '''
    post_fields = {'deviceid': '%'            
                   }
    
    #Encode the parameters
    post_data = urllib.urlencode(post_fields)
    
    #Submit the form (POST request). You get the post_url and the request type(POST/GET) the same way with the parameters.
    resp = br.open(search_button_url,post_data)
    time.sleep(0.2)
    '''
    resp = br.open(meter_names)
    time.sleep(1.0)
    
    meter_list = meter_list + re.findall(r'\?deviceId=(.*?)&',resp.read(),re.DOTALL)
    
    current_meter_ix = current_meter_ix + METER_IX_INCR

with open(OUTFILE_NAME, 'wb') as outfile:
    outfile.write(','.join(meter_list) + '\n')
   
a=2 


