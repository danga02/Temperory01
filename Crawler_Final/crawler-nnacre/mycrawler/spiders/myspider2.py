from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import base64
import re
#input_var = input("Enter something: myspider \n")
from mycrawler.items import MycrawlerItem
import dateutil.parser
import requests
import json
import sys
import demjson
import math
import MySQLdb
import set12

class MySpider(Spider):
    name = 'nnacre'
    allowed_domains = ['99acres.com']
    #city_names=['gurgaon', 'noida', 'mumbai', 'pune', 'bangalore', 'chennai', 'kolkata', 'hyderabad', 'ahmedabad', 'delhi-ncr']
    #buy_url=['http://www.99acres.com/property-in-%s-ffid-page-1'%(city) for city in city_names]
    #buy_url=['http://www.99acres.com/property-in-noida-ffid-page-1']
    
    '''#self.conn = MySQLdb.connect(host='localhost', user='root', passwd='ROOT@123', db='crawling')
    #self.conn = MySQLdb.connect(host='ec2-54-255-51-21.ap-southeast-1.compute.amazonaws.com', user='root', passwd='ROOT@123', db='crawling') '''
    #conn = MySQLdb.connect(host='192.168.1.1', user='root', passwd='root', db='cms')
    conn = set12.conn1    
    cursor = conn.cursor() 
    
    if cursor == 0:
        print 'not connetcted','\n' 
    else :
        print 'Connected !!!!!!' ,'\n'

    #input_var = input("Enter data \n")

    qry = 'select builder_name, project_name, l.label, sb.CITY_ID, PROJECT_ID from resi_project rp join resi_builder rb on rp.builder_id = rb.builder_id join locality l on l.LOCALITY_ID = rp.locality_id join suburb sb on sb.SUBURB_ID = l.SUBURB_ID where rp.version = "Website" and sb.CITY_ID = 2'
    #q1 = 'select builder_name, project_name, l.label from resi_project rp join resi_builder rb on rp.builder_id = rb.builder_id join locality l on l.LOCALITY_ID = rp.locality_id where rp.version=Website group by project_id'
 
    try:

        cursor.execute(qry)
        rows = cursor.fetchall()
        cnt = 0
        sub = ''
        pid = ''
        tl = 4      # here tl is how many projects u want to crawl currently it's 5  !!!!!!
        temp = []
        pid_arr = []
        count_pid = 0
        count_pid_max = 0
        increase_count = true
        for rw in rows:
            print rw[4],' -- ',rw[1], '\n'
            #sub = rw[1]
            pid = rw[4]
            bn = rw[0]   
            pn = rw[1]
            lab = rw[2]
            sub = bn + ' ' + pn+ ' ' + lab
   
            if cnt == tl or sub == '':
                break
            
            #ul = 'https://www.googleapis.com/customsearch/v1?key=AIzaSyD7vrV34kZR_m7vYOfIecX4_9Hksvj1eo8&cx=014024166646265731685:cqmm9wkynuq&q='
            ul = 'https://www.googleapis.com/customsearch/v1?key=AIzaSyAiKeTNZBZ8q70K53u7atOmsS73j67e3C0&cx=007606135028501870088:me8goulm6us&q='    
            url = ul + sub
            try:
                r = requests.get(url)
                store_page = r.json()
                data = store_page['items']
                d1 = data[0]
                
                final = d1['link']
                print 'cnt = ', cnt, ' --- ', final,'\n'
                pid_arr.append(pid)
                count_pid_max +=1
                temp.append(final)	
                cnt += 1  
            except:
            	pass

        start_urls = temp            	
        #print start_urls,'\n'

        #input_var = input("Enter something: myspider- pipelinese3wqerwr \n")
        # set the number of pages wants to crawl currently it's 100  (x = 100)!!!!!!  
        x = 100  
        ind = 0
        flag = 1 
		#input_var = input("Enter something: myspider- pipelinese3wqerwr \n")
        def parse(self, response):
            print 'hello'
			print 'parse calling - ',response.url,'\n\n'
			sel = Selector(response)
			urls = sel.xpath('//a/@href').extract()
			count = 0
			for url in urls:
                #print response.url
			    if "-spid-" in url:
			        count = count + 1
			        full_url = 'http://www.99acres.com' + url
			        full_url = full_url.split('&')[0]
			        print 'count - ',count, ' -- ', full_url,'\n'
			        yield Request(full_url, callback=self.parse2)

			next_link_checker=sel.xpath('//div[@class="pgdiv"]/input[@name="page"]/@value').extract()   

			val = 0                   
			try :
			    val = next_link_checker[-1]
			except :
			    self.ind = self.x + 1
			    pass

			#input_var = input("Enter something: myspider- pipelinese3wqerwr feywufwe\n")

			#if next_link_checker!=[u'Next \xbb']:
			if self.ind <= val and self.ind <= self.x and count != 0:
			    next_link = self.start_urls[0] + '?page=' + str(self.ind)
			    #input_var = input("Enter something: myspider- pipelinese3wqerwr \n")
			    print '\n\n ##################### next_link = ', next_link ,'  #################################','\n\n'
			    self.ind = self.ind + 1 
                self.increase_count = false    
			    yield Request(next_link, callback=self.parse)  

            self.increase_count = true

    

        def parse2(self, response):
            spid = response.url
            spid = spid.split('-')[-1] 
            sel = Selector(response)
            Apartment_name = sel.xpath('//h1[@class="prop_seo_head f16 b"]/text()').extract()
            address = sel.xpath('//div[@class="fwn f13 addPdElip"]/text()').extract()
            Floor_number = sel.xpath('//div[@class="spdp_blCny f13 fwn"]/i[@class="blk rel"]/text()').extract()
            Possession = sel.xpath('//div[@class="lf"]/div[@class="spdp_blCny f13 fwn"]/i[@class="blk"]/text()').extract()
            encoded_string = sel.xpath('//a[@class="bLink lf p5 mt10"]/@onclick').extract()
            data = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split('\n')[0]
            rate = sel.xpath('//span[@id="price_per_unit_areaLabel"]/text()').extract()     
            rate1 = sel.xpath('//span[@class="redPd b"]/text()').extract() 
            posted_date = sel.xpath('//span[@class="rf PostdByPd mt3 f13 blk"]/text() | //span[@class="rf PostdByPd mt3 f13 "]/text()').extract()
            property_code = response.url.split('-')[-1].split('&')[0]
            posted_by = sel.xpath('//div[@class="dpDetail"]/span[@class="grey f13"]/text()').extract()
            owner_type = sel.xpath('//div[@class="dpDetail"]/a[@id="ContactPdBody"]/text()').extract()        
            owner_address = sel.xpath('//div[@class="f13 pdt10"]/span/text()').extract()
         
            BHK = sel.xpath('//div[@id="bedroom_numLabel"]/b/text()').extract()
            i = 0
            bh = ''
            while i < len(BHK[0]):
                if BHK[0][i].isdigit() :
                    bh = bh + BHK[0][i]
                i = i + 1            
           
            items = []
            item = MycrawlerItem()
            data = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split('\n')[0]
                   
            try:
                st = ''
                st = Apartment_name[0].split(' for ')[0]
                item['property_type'] = st
            except IndexError:
                item['property_type'] = ''

            try:
                st = ''
                st = Apartment_name[0].strip().split(' in ')[-1].split(',')[0]
                item['Apartment_name'] = st
            except IndexError:
                item['Apartment_name'] = ''

            try:
                st = ''
                st = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split(' in ')[-1].split('.')[0]
                item['locality'] = st
            except IndexError:
                item['locality'] = ''

            try:
                st = ''
                st = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split('\n')[0].split(' ')[-1].split('.')[0]
                item['city'] = st
            except IndexError:
                item['city'] = ''

            try:
                st = ''
                st = address[0].split('Address: ')[-1].strip()
                item['address'] = st
            except IndexError:
                item['address'] = ''

            try:
                pfloor_number = Floor_number[1].strip()
                if 'Ground' in pfloor_number:
                    st = '0'
                    item['Floor_number'] = st    
                else:
                    st = ''
                    st = Floor_number[1].strip()
                    item['Floor_number'] = st
            except IndexError:
                item['Floor_number'] = '-1'
        
            try:
                st = ''
                st = Possession[0].split(': ')[-1]
                item['Possession'] = st
            except IndexError:
                item['Possession'] = ''

            try:
                st = ''
                st = Possession[1].split(': ')[-1]
                item['Property_age'] = st
            except IndexError:
                item['Property_age'] = ''

            try:    
                st = ''
                st = Possession[2].split(': ')[-1]
                item['Transaction_Type'] = st
            except IndexError:
                item['Transaction_Type'] = ''

            try:
                st = ''
                st = Possession[3].split(': ')[-1]
                item['Property_Ownership'] = st
                    
            except IndexError:
                item['Property_Ownership'] = ''
                
            try:
                st = sel.xpath('//div[@class="lf mt15"]/b/text()').extract()
                i = 0
                btr = ''
                while i < len(st[0]):
                    if st[0][i].isdigit() :
                        btr = btr + st[0][i]
                    i = i + 1 
                item['bathroom'] = btr
            except IndexError:
                item['bathroom'] = ''
        
            try:
                parea = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split(' for ')[-1].split(' ')[1]            
              
                st = ''
                st11 = sel.xpath('//span[@class="lf mt5"]/i[@id="superbuiltupArea_span"]/text()').extract()
                st12 = sel.xpath('//span[@class="lf mt5"]/i[@id="builtupArea_span"]/text()').extract()
                f = 0
                if len(st11) > 0:
                    st = st11[0]
                elif len(st12) > 0:    
                    st = st12[0] 
                else :
                    f = 1 
                
                ss = ''
                if f == 0:
                    i = 0
                    while i < len(st):
                        if (st[i].isdigit() or st[i] == '.') :
                            ss = ss + st[i]
                        i = i + 1             

                item['area'] = ss
                
                st = ''
                st21 = sel.xpath('//span[@id="superbuiltupAreaLabel"]/text()').extract()
                st22 = sel.xpath('//span[@id="builtupAreaLabel"]/text()').extract()
                if len(st21) > 0 :
                    st = st21[0]
                elif len(st22) > 0:
                    st = st22[0]

                item['area_unit'] = st
                
                  
            except IndexError:
                item['area'] = ''
                #pass 

            try:
                prs = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split('\n')[0].split(' Rs.')[1].split(' ')[0]
                
                if 'L' in prs:
                    pl = prs.split('L')[0]
                    psl = float(pl)
                    psl = psl * 100000.0
                    item['price'] = psl
                    
                if 'C' in prs:
                    pl = prs.split('C')[0]
                    psl = float(pl)
                    psl = psl * 10000000.0
                    item['price'] = psl            
                    
                
            except IndexError:
                item['price'] = ''

            try:
                rt = float(item['price'])/float(item['area'])
                item['rate'] = math.ceil(rt*100)/100 
                
            #except IndexError, ValueError:
            except:
                item['rate'] = '-1' 
     
            try:
                item['contact_person'] = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split(' contact ')[-1].split(' at ')[0]            
                        
            except IndexError:
                item['contact_person'] = ''

            try:
                item['contact_number'] = base64.b64decode(encoded_string[0].split("','")[13]).split('\n')[0].split(' at ')[-1].split(' for ')[0]
                
            except IndexError:
                item['contact_number'] = ''

            item['url'] = response.url
            item['url_referer'] = response.request.headers.get('Referer')

            pposted_date = posted_date[0].split('Posted on:')[-1]
            item['posted_date'] = dateutil.parser.parse(pposted_date).strftime('%Y-%m-%d')
            
            item['property_code'] = property_code
            item['BHK'] = bh
            if '-Rent-' in response.url:
                item['ptype'] = 'Rent'
            else:
                item['ptype'] = 'Buy'

            item['posted_by'] = posted_by[0].split('Posted by')[-1] 
            item['owner_type'] = owner_type[0].split('Contact')[-1]
            try:
                item['owner_address'] = owner_address[0]
            except IndexError:
                item['owner_address'] = ''
            
            item['crawl_cycle'] = ''
            item['time_stamp'] = 'now()'
            item['pt_project_id'] = self.pid
            item['pt_type_id'] = ''
            item['is_outlier'] = ''
            item['to_CMS'] = '' 

            items.append(item)

            print 'size of array = ', len(item),'\n'

            return items     
        
    except:
    	#print e
    	print 'error ocure \n'
    	conn.rollback()            

  
		    
