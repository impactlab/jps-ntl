import mechanize
import cookielib
import os,time
from BeautifulSoup import BeautifulSoup

# Parameters
svc_domain = r'http://localhost:9090/'
svc_home = svc_domain + 'serview.jsp'
svc_login = svc_domain + 'home_D.jsp'

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

# The site we will navigate into, handling it's session
resp = br.open(svc_home)

time.sleep(0.2)

resp = br.open(svc_login)

# View available forms
for f in br.forms():
    print f


# Select the second (index one) form (the first form is a search query box)
br.select_form(nr=0)

# User credentials
br.form['j_username'] = user_name
br.form['j_password'] = user_pass

# Login
resp = br.submit()


