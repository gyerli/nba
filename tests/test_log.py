#!/usr/bin/python

import common as c
import time
import os

import mechanize
import cookielib
#from BeautifulSoup import BeautifulSoup
from bs4 import BeautifulSoup
import html2text

def select_form(form):
  return form.attrs.get('id', None) == 'siteSignupForm'

#def select_form(form):
#  return form.attrs.get('action', None) == '/Account/SiteLogin'

br = mechanize.Browser(factory=mechanize.RobustFactory())
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

# Browser options
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

br.addheaders = [('User-agent', 'Chrome')]

# c.log.info("This is a test!")
# c.log.debug("This is a debug test!")

url = 'https://www.draftkings.com/account/sitelogin'

# The site we will navigate into, handling it's session
br.open(url)

soup = BeautifulSoup(br.response().read())
html = str(soup)
resp = mechanize.make_response(html, [("Content-Type", "text/html")],
                               br.geturl(), 200, "OK")
br.set_response(resp)
br.select_form(predicate=select_form)
