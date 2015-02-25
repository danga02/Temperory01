#from sqlalchemy import create_engine
import MySQLdb
import spiders.set12
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html



class mycrawlerSqlPipeline(object):
    def __init__(self):
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='ROOT@123', db='crawling')
        #self.conn = MySQLdb.connect(host='ec2-54-255-51-21.ap-southeast-1.compute.amazonaws.com', user='root', passwd='ROOT@123', db='crawling')
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='proptiger', db='crawling')
        self.conn = spiders.set12.conn2
        self.cursor = self.conn.cursor()
        
    
    def process_item(self, item, spider):
        '''print 'url  -->  ',item['url'],'\n'
        print 'url_referer  ->  ', item['url_referer'],'\n' 
        print 'Transaction_Type  -->  ',item['Transaction_Type'],'\n'
        print 'price  -->  ', item['price'],'\n'
        print 'rate  -->  ',item['rate'],'\n'
        print 'address  -->  ', item['address'],'\n'
        print 'property_type  -->  ',item['property_type'],'\n'
        print 'Prossession  -->  ',item['Possession'],'\n' 
        print 'City  -->  ',item['city'],'\n'
        print 'conatact_number  -->  ',item['contact_number'],'\n'
        print 'locality -->  ',item['locality'],'\n'
        print 'area -->  ', item['area'],'\n'
        print 'area_unit -->  ',item['area_unit'],'\n'
        print 'Property_Ownership  -->  ',item['Property_Ownership'],'\n'
        print 'Bathroom  -->  ' , item['bathroom'],'\n' 
        print 'Floor_number  -->  ',item['Floor_number'],'\n'
        print 'Property_age  -->  ',item['Property_age'],'\n'
        print 'Apartment_name -->  ',item['Apartment_name'],'\n'
        print 'contact_person  -->  ',item['contact_person'],'\n'
        print 'posted_date  -->  ', item['posted_date'],'\n'
        print 'property_code  -->  ',item['property_code'],'\n' 
        print 'BHK  -->  ',item['BHK'],'\n'
        print 'owner_address  -->  ',item['owner_address'],'\n' 
        print 'owner_type  -->  ',item['owner_type'],'\n'
        print 'posted_by  -->  ', item['posted_by'],'\n' 
        print 'ptype  -->  ',item['ptype'],'\n'           
        print 'pid -----> ',item['pt_project_id'],'\n'  '''
        #input_var = input("Enter something: myspider- pipelines \n")
        '''query1 = 'INSERT INTO  rsl12 (url,url_referer,transaction_type,price,rate,address,property_type,possession,city,contact_number,locality,area,area_unit,property_ownership,floor_number,property_age,apartment_name,contact_person,posted_date,property_code,BHK,owner_address,owner_type,posted_by,ptype,bathroom)'
        query2 = 'values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s") ' % (
            item['url'], item['url_referer'], item['Transaction_Type'], item['price'],item['rate'], item['address'],item['property_type'], item['Possession'], 
            item['city'], item['contact_number'], item['locality'],item['area'],item['area_unit'], item['Property_Ownership'], item['Floor_number'], item['Property_age'],
            item['Apartment_name'], item['contact_person'], item['posted_date'], item['property_code'], item['BHK'],item['owner_address'], item['owner_type'], 
            item['posted_by'], item['ptype'], item['bathroom'])'''

        #query3='ON DUPLICATE KEY UPDATE '
        #query4='url_referer="%s",transaction_type="%s",price="%s",rate="%s",address="%s",property_type="%s",possession="%s",city="%s",contact_number="%s",locality="%s",area="%s",area_unit="%s",property_ownership="%s",floor_number="%s",property_age="%s",apartment_name="%s",contact_person="%s",posted_date="%s",property_code="%s",BHK="%s",owner_address="%s",owner_type="%s",posted_by="%s",ptype="%s"'%(item['url_referer'],item['Transaction_Type'],item['price'],item['rate'],item['address'],item['property_type'],item['Possession'],item['city'],item['contact_number'],item['locality'],item['area'],item['area_unit'],item['Property_Ownership'],item['Floor_number'],item['Property_age'],item['Apartment_name'],item['contact_person'],item['posted_date'],item['property_code'],item['BHK'],item['owner_address'],item['owner_type'],item['posted_by'],item['ptype'])
        #print query1 + query2
        #try:
        #self.cursor.execute('INSERT INTO  crawled_data (url,url_referer,transaction_type,price,rate,address,property_type,possession,city,contact_number,locality,area,area_unit,property_ownership,floor_number,property_age,apartment_name,contact_person,posted_date,property_code,BHK,owner_address,owner_type,posted_by,ptype) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'%(item['url'],item['url_referer'],item['Transaction_Type'],item['price'],item['rate'],item['address'],item['property_type'],item['Possession'],item['city'],item['contact_number'],item['locality'],item['area'],item['area_unit'],item['Property_Ownership'],item['Floor_number'],item['Property_age'],item['Apartment_name'],item['contact_person'],item['posted_date'],item['property_code'],item['BHK'],item['owner_address'],item['owner_type'],item['posted_by'],item['ptype']))
    
        query1 = 'INSERT INTO  crawled_data_new (crawl_cycle,time_stamp,url,url_referer,transaction_type,price,rate,address,property_type,possession,city,contact_number,locality,area,area_unit,property_ownership,floor_number,property_age,apartment_name,contact_person,posted_date,property_code,BHK,owner_address,owner_type,posted_by,ptype,pt_project_id,pt_type_id,is_outlier,to_CMS)'
        query2 = 'values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s") ' % (
            item['crawl_cycle'],item['time_stamp'],item['url'],item['url_referer'], item['Transaction_Type'], item['price'],item['rate'], item['address'],item['property_type'], item['Possession'], 
            item['city'], item['contact_number'], item['locality'],item['area'],item['area_unit'], item['Property_Ownership'], item['Floor_number'], item['Property_age'],
            item['Apartment_name'], item['contact_person'], item['posted_date'], item['property_code'], item['BHK'],item['owner_address'], item['owner_type'], 
            item['posted_by'], item['ptype'], item['pt_project_id'],item['pt_type_id'],item['is_outlier'],item['to_CMS'])

        
        try:
            self.cursor.execute("set time_zone='+05:30'")
            self.cursor.execute(query1 + query2)
            self.conn.commit()
        except:
            #print e
            self.conn.rollback()
        #self.conn.close()
        #self.conn = MySQLdb.connect(*settings.MYSQLDB_CONNARGS)
        #input_var = input("Enter something: myspider- pipelines \n")
        return item
