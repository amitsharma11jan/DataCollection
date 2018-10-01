
# coding: utf-8

# ### Data Collection Group Assignment
# 
# #### Goal:<br>
# To make students understand basic scraping, and use simple heuristics to handle real world unclean web data.
# 
# #### Task:<br>
# You are provided 1000 Yelp business pages downloaded from yelp.com. Also there is a file called urls.txt which contains URLs corresponding to each of those 1000 pages. This data is available here: https://drive.google.com/open?id=0B1jCYNXvevLobTlUV2dpbndETWc <br><br>
# Each webpage is an HTML containing details about the business. It does not have the email id, but it has the website address for the business which can be used to find the contact us page for the website and thereby extract its email id. Your task is to obtain structured data for the business: business name, business phone number, business home page URL, business address, opening hours, Takes Reservations, Delivery, Take-out, Accepts Credit Cards, Accepts Apple Pay, Accepts Android Pay, Accepts Bitcoin, Good For, Parking, Bike Parking, Good for Kids, Good for Groups, Attire, Ambience, Noise Level, Alcohol, Outdoor Seating, Wi-Fi, Has TV, Caters, Gender Neutral Restrooms, contact-us URL for the business, email id for the business.
# 


# In[1]:

import os
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
from urllib.request import Request, urlopen
from urllib.parse import urlsplit
import threading
import time
import sys
import urllib.request


# In[2]:

os.chdir("/Users/a5sharma/Documents/ISB/DC/Assignment/")
yelpFilesPath = "/Users/a5sharma/Documents/ISB/DC/GroupAssignment/assignmentData/"
resultFilePath = "/Users/a5sharma/Documents/ISB/DC/Assignment/"


# #### extractBusinessInformation method:
# 
# This method read data from downloaded Yelp pages and extract information about downloaded pages. This will create result.csv file with 1000 records.

# In[3]:

def extractBusinessInformation(yelpFilesPath):
    final = pd.DataFrame()
    for i in range(1,1001):
        url = yelpFilesPath+str(i)+".html"
        with open(url) as fp:
            soup = BeautifulSoup(fp, 'lxml')
        map_t = {}
        opening_hour = ''
        if len(soup.select('.biz-page-title')) == 0:
            busines_name = None
        else:
            busines_name = soup.select('.biz-page-title')[0].text.strip().replace('’',"'")
        if len(soup.select('.biz-phone')) == 0:
            busines_phone = None
        else:
            busines_phone = soup.select('.biz-phone')[0].text.strip()

        if len(soup.select('address')) == 0:
            busines_address = None
        else:
            busines_address = soup.select('address')[0].text.strip()
        map_t['business name'] = busines_name
        map_t['business phone number'] = busines_phone
        map_t['business address'] = busines_address

        if len(soup.find_all('div',class_='ywidget biz-hours'))==0:
            map_t['opening hours'] = None
        else:
            if len(soup.find('div',class_='ywidget biz-hours').find_all(class_='table table-simple hours-table')) != 0:
                for k in soup.find('div',class_='ywidget biz-hours').find(class_='table table-simple hours-table').find_all('tr'):
                    if k.find('th') != None:
                        if opening_hour =='':
                            opening_hour = k.find('th').get_text().strip()+":"+k.find('td').get_text().strip()
                        else:
                            opening_hour = opening_hour+','+k.find('th').get_text().strip()+":"+k.find('td').get_text().strip()
                    map_t['opening hours'] = opening_hour
            else:
                map_t['opening hours'] = None

        if len(soup.select('.biz-website a')) == 0:
            map_t['business home page URL'] = None
        else:
            #map_t['business home page URL'] = "http://"+soup.select('.biz-website a')[0].text.strip()
            biz_website_url = soup.select_one('.biz-website a').attrs['href']
            biz_website = biz_website_url[biz_website_url.index('url=')+4:biz_website_url.index('website_link_type')-1]
            biz_website = biz_website.replace('%2F','/').replace('+',' ').replace('%3A', ':').replace('%7C','|').replace('%23','#').replace('%21','!').replace('%3F','?').replace('%3D','=').replace('%26','&')
            if '…' in soup.select('.biz-website a')[0].text or 'www.' in biz_website_url:
                map_t['business home page URL'] = biz_website
            else:
                if(biz_website.find('://') > 0):
                    protocol = biz_website.split('://')[0]+"://"
                else:
                    protocol = ''
                map_t['business home page URL'] = protocol+soup.select('.biz-website a')[0].text.strip()

        #print(i)

        for k in soup.find_all(class_='ywidget'):
            for j in k.find_all('ul',class_='ylist'):
                if len(j.find_all('div',class_='short-def-list')) > 0:
                    for m in j.find_all('dl'):
                        map_t[m.select_one('dt').text.strip()] = m.select_one('dd').text.strip()

        map_t['index'] = i
        columns = list(map_t.keys())
        values  = list(map_t.values())
        arr_len = len(values)
        df = pd.DataFrame(np.array(values, dtype=object).reshape(1, arr_len), columns=columns, index=[i])
        if i == 1:
            final = df
        else:
            final = pd.concat([final, df])
    final[['index','business name', 'business phone number', 'business home page URL', 'business address', 'opening hours', 'Takes Reservations', 'Delivery', 'Take-out', 'Accepts Credit Cards', 'Accepts Apple Pay', 'Accepts Android Pay', 'Accepts Bitcoin', 'Good For', 'Parking', 'Bike Parking', 'Good for Kids', 'Good for Groups', 'Attire', 'Ambience', 'Noise Level', 'Alcohol', 'Outdoor Seating', 'Wi-Fi', 'Has TV', 'Caters', 'Gender Neutral Restrooms']].to_csv(resultFilePath+'result.csv',index=None)


# In[ ]:

print("Starting ",time.ctime(),)
extractBusinessInformation(yelpFilesPath)
print("Ending",time.ctime(),)


# #### extractEmail method
# 
# This method is used to extract email ids from business home page or restaurant contact page. Input for this method is restaurant contact page or business home page.
# 
# This method find 'email address' using the regular expression "u"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-{2,6}]+", re.IGNORECASE".  
# 
# If it finds valid email address, it will store email ids in Set data structure to ignore duplicate email address and return all email ids in comma seperated string.
# 
# If it finds invalid email address like image file name or css class name as email address, then this method will ignore these invalid email address.

# In[4]:

def extractEmail(text):
    #Set to store unique email address
    emailString = None
    reg_ex=re.compile(u"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-{2,6}]+", re.IGNORECASE)
    
    temp=reg_ex.findall(text.decode('utf-8'))
    if len(temp)>=1:
        emailSet = set()
        for i in temp:
            #Store only valid email address in set
            if '.png' not in i.lower() and '.jpg' not in i.lower() and '.here' not in i.lower() and '.svg' not in i.lower():
                k = i.lower().find('.com')
                if(k > 0):
                    emailSet.add(i[0:k+4].lower())
                else:
                    t = i.split('@')[1].split('.')
                    if(len(t[len(t)-1]) > 1):
                        emailSet.add(i.lower())
        email = list(emailSet)
        emailString = ','.join(email)
    return emailString


# #### extractContactUrl method:
# 
# This method is used to extract contact url from business home page. Input for this method is business home page.<br>
# 
# This method extract the href value from the anchor tag and find the word 'contact' using the regular expression "r'.contact.',re.IGNORECASE". If it found 'contact' keyword in anchor text or href, then return href value. This method will return None if it does not find out 'contact' keyword.
# 
# Some webpages have javascript function and email address in href, then this method return None.

# In[5]:

def extractContactUrl(text):
    contactUrl = None
    searchText = []
    regEx=re.compile(r'.*contact.*',re.IGNORECASE)
    for i in text.find_all('a'):
        searchText = regEx.findall(i.getText())
        if len(searchText)==0:
            try:
                searchText = regEx.findall(i.get("href"))
            except Exception as e:
                searchText = []
        if len(searchText) >0:
            contactUrl = i.get("href")
            break
    if contactUrl is not None:
        #some pages have email address in home page and contact is given as email address
        if 'mailto' in contactUrl or 'javascript' in contactUrl:
            contactUrl = None
            return contactUrl
        if ('http' in contactUrl or 'https' in contactUrl):
            return contactUrl
        if contactUrl[0] != '/' and contactUrl[0] != '#':
            contactUrl = "/"+contactUrl
    return contactUrl


# #### extractEmailAndContact method:
# 
# This method is used to extract email address and contact url from the business home page. This method get 'business home page URL' from result.csv file and download business home page in local variable. 
# 
# This method pass the downloaded business home page to extractContactUrl method to get contact url. 
# 
# Also, this method download contact webpage if contact page exixts and pass the downloaded contact webpage to extractEmail method if contact page exists else pass business home page if contact page does not exist to extractEmail method to get email ids.
# 
# This method will store email ids and contact us URL in temporary files.

# In[6]:

def extractEmailAndContact(threadName, lowerIndex, upperIndex, delay):
    newDf = df[(df.index >= lowerIndex) & (df.index < upperIndex)]
    final_contact = pd.DataFrame()
    for index, row in newDf.iterrows():
        dict_final = {}
        contact_us = ""
        email = ""
        time.sleep(delay)
        try:
            #print(threadName,index)
            #read content of homepage
            req = Request(row[3], headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'})
            page = urlopen(req).read()

            soup = BeautifulSoup(page, 'lxml')
            contactUrl=extractContactUrl(soup)
            dict_final['index'] = row[0]
            if contactUrl is None:
                contactUrl = None
                contact_us = row[3]
            elif contactUrl is not None and 'http' not in contactUrl:
                homePage = row[3]
                if(homePage[len(homePage)-1] =='/'):
                    contact_us = homePage[0:len(homePage)-1]+contactUrl
                else:
                    contact_us = homePage+contactUrl
            elif ('http' in contactUrl or 'https' in contactUrl):
                contact_us = contactUrl
            else:
                contactUrl = None
                contact_us = row[3]  
            dict_final['contact-us URL for the business'] = contact_us
            #if contact page present find email from the contact page    
            if contactUrl is not None:
                try:
                    req1 = Request(contact_us, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'})
                    urlContent = urlopen(req1).read()
                    soupcontact = BeautifulSoup(urlContent, 'lxml')
                    email = extractEmail(soupcontact)
                    if email == None:
                        email = extractEmail(soup)
                    dict_final['email id for the business']=email
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        try:
                            url = contact_us
                            homePage = "{0.scheme}://{0.netloc}/".format(urlsplit(url))
                            if(homePage[len(homePage)-1] =='/'):
                                contact_us = homePage[0:len(homePage)-1]+contactUrl
                            else:
                                contact_us = homePage+contactUrl
                            dict_final['contact-us URL for the business'] = contact_us
                            req1 = Request(contact_us, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'})
                            urlContent = urlopen(req1).read()
                            soupcontact = BeautifulSoup(urlContent, 'lxml')
                            email = extractEmail(soupcontact)
                            if email == None:
                                email = extractEmail(soup)
                            dict_final['email id for the business']=email
                        except Exception as e1:
                            print(threadName,index,row[3],contact_us,'exception0 :',e1)
                            email = extractEmail(soup)
                            dict_final['email id for the business'] = email
                    else:
                        print(threadName,index,row[3],contact_us,'exception1 :',e)
                        email = extractEmail(soup)
                        dict_final['email id for the business'] = email
                except Exception as e:
                    print(threadName,index,row[3],contact_us,'exception2 :',e)
                    email = extractEmail(soup)
                    dict_final['email id for the business'] = email
            else:
                email = extractEmail(soup)
                dict_final['email id for the business']=email
        except Exception as e2:
            print(threadName,index,row[3],contact_us,'exception3 :',e2,)
            dict_final['contact-us URL for the business'] = None
            dict_final['email id for the business'] = None
        columns = list(dict_final.keys())
        values  = list(dict_final.values())
        arr_len = len(values)
        df1 = pd.DataFrame(np.array(values, dtype=object).reshape(1, arr_len), columns=columns)
        if len(final_contact) ==0:
            final_contact = df1
        else:
            final_contact = pd.concat([final_contact, df1])
    final_contact.to_csv(resultFilePath+threadName+'.csv',index=None)


# #### Create myThread class.
# This class is used to create thread.

# In[7]:

#Extracting Email And Contact information take more than 2 hours to hit 1000 websites, 
#that's why I am launching 50 parallel thread at a time and it finish within 35 min to extract contact url 
#and email address from 1000 webpages.
class myThread (threading.Thread):
    def __init__(self, threadID, name, lowerIndex, upperIndex):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.lowerIndex = lowerIndex
        self.upperIndex = upperIndex
    def run(self):
        print("Starting " + self.name,)
        extractEmailAndContact(self.name, self.lowerIndex, self.upperIndex, 100)
        print("Exiting " + self.name,)


# #### Create 50 threads for parallel execution.

# In[8]:

i=0
threadCount = 1
df = pd.read_csv(resultFilePath+'result.csv')
df=df[df['business home page URL'].isnull()==False]
threadLock = threading.Lock()
threads = []
for j in range(0,1001,20):
    if i == 0 and j != 0:
        thread = myThread(threadCount, "Thread-"+str(threadCount), i, j)
        thread.start()
        threads.append(thread)
        threadCount = threadCount+1
    if i != 0 and j != 0:
        thread = myThread(threadCount, "Thread-"+str(threadCount), i, j)
        thread.start()
        threads.append(thread)
        threadCount = threadCount+1
    i = j


# Launch 50 threads. Each thread will execute extractEmailAndContact method for 20 records and will create 50 temporary files.

# In[9]:

print("Starting Main Thread",time.ctime(),)
for t in threads:
    t.join()
print("Exiting Main Thread",time.ctime(),)


# Read email id and contact us URL from temporary files and merge all 50 temporary files in one file. 

# In[10]:

df = pd.read_csv(resultFilePath+'result.csv')
for i in range(1,51):
    if i == 1:
        df1 = pd.read_csv(resultFilePath+'Thread-'+str(i)+'.csv')
        os.remove(resultFilePath+'Thread-'+str(i)+'.csv')
    else:
        df2 = pd.read_csv(resultFilePath+'Thread-'+str(i)+'.csv')
        df1 = pd.concat([df1,df2],axis=0)
        os.remove(resultFilePath+'Thread-'+str(i)+'.csv')
df=df.merge(df1,how="left")
df.drop('index', axis=1, inplace=True)


# In[11]:

df.to_csv(resultFilePath+"result.csv",sep=",",index=None)


# In[12]:

df.to_csv(resultFilePath+"result.txt",sep="\t",index=None)


# In[ ]:



