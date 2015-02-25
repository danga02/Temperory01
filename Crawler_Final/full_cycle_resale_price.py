This is new file added
import danga
import saksajidh
import git
import oneline wdded
import subprocess
import os
import MySQLdb
import boto
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

#from scrapy.spider import Spider
#from scrapy.selector import Selector

import csv
import re
import numpy
from scipy.stats import mode
from PyAstronomy import pyasl
import requests
import json
import datetime

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

#db = MySQLdb.connect(host="ec2-54-255-51-21.ap-southeast-1.compute.amazonaws.com", user="root", passwd="ROOT@123", db="crawling")
#db = MySQLdb.connect(host="localhost", user="root", passwd="ROOT@123", db="crawling")
db = MySQLdb.connect(host="localhost", user="root", passwd="proptiger", db="crawling")
cursor = db.cursor()
cursor.execute('set wait_timeout=288000')

#current_crawling_cycle = find_last_crawl_cycle() + 1
#current_crawling_cycle=3

# to find out last crawled_cycle
def find_last_crawl_cycle():
    cursor.execute('select max(crawl_cycle) from crawled_data')
    result = cursor.fetchall()
    return int(result[0][0])

def error_notifier(message):
    send_mail('ashok.danga@proptiger.com', 'error in resale price project321323248923rhwehfjfidf', message)

def send_mail(send_to, subject, mail_body, file_name='no'):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'no-reply@proptiger.com'
    msg['To'] = send_to

    # what a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'

    # the message body
    part = MIMEText(mail_body)
    msg.attach(part)

    # the attachment
    if file_name != 'no':
        part = MIMEApplication(open(file_name, 'rb').read())
        part.add_header('Content-Disposition', 'attachment', filename=file_name)
        msg.attach(part)

    # connect to SES
    aws_access_key_id = 'AKIAIPT74FHV5KIH6CBA'
    aws_access_key_secret = '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'
    connection = boto.connect_ses(aws_access_key_id, aws_access_key_secret)

    # and send the message
    result = connection.send_raw_email(msg.as_string(), source=msg['From'], destinations=[msg['To']])
    print result


def start_crawling(current_crawl_cycle):
    # send email which crawlers are starting
    #start nnacre
    os.chdir(BASE_DIR + '/crawler-nnacre')
    # CSV file of crawled data for future
    subprocess.call(['scrapy', 'crawl', 'nnacre', '-o', 'crawled_data_cycle_%s'%(current_crawl_cycle), '-t', 'csv'])
    #input_var = input("Enter something: fulll  - start crawling !!!! see outside subprocess\n")
    os.chdir(BASE_DIR)


#start magicbricks

#start commonfloor

# match from lookup table
def matcher_from_lookup_table(apartment_name, locality, city):
    try:
        cursor.execute('select project_id from lookup_table where apartment_name="%s" and locality="%s" and city="%s"' % (apartment_name, locality, city))
    except:
        print 'error in ', (apartment_name, locality, city)
        send_mail('ashok.danga@proptiger.com', 'error in %s'%(str((apartment_name, locality, city))), 'error in %s'%(str((apartment_name, locality, city))))
        return ()

		
    result = cursor.fetchall()
    print result
    return result


# push matched PID to db
def push_found_pid_to_crawled_data(apartment_name, locality, city, project_id, current_crawl_cycle):
    query = 'UPDATE crawled_data SET pt_project_id="%s" where apartment_name="%s" and locality="%s" and city="%s" and pt_project_id is null and crawl_cycle="%s"' % (
        project_id, apartment_name, locality, city, current_crawl_cycle)
    try:
        cursor.execute(query)
        db.commit()
    except:
        db.rollback()

    print (apartment_name, locality, city, project_id), 'pushed to crawled_data'


# solr results giver
def solr_fetch(query, city):
    url1 = 'http://guest:12345@beta.proptiger-ws.com:8983/solr/collection1/select?q='
    url2 = '"%s"' % query
    url3 = '&fq=DOCUMENT_TYPE%3APROPERTY&fq=CITY%3A '
    url4 = city
    url5 = '&start=0&rows=15&fl=PROJECT_ID+PROJECT_NAME+LOCALITY+CITY&wt=json&indent=true'
    final_url = url1 + url2 + url3 + url4 + url5
    #print final_url
    r = requests.get(final_url)
    results = r.json()['response']['docs']
    #print results
    return results


# match from solr
def solr_matcher(apartment_name, locality, city):
    try:
        city_name_clean = \
            re.findall('Gurgaon|Noida|Mumbai|Pune|Bangalore|Chennai|Kolkata|Delhi|Ghaziabad|Ahmedabad', city,
                       re.IGNORECASE)[0]
    #print city_name_clean
    except KeyboardInterrupt:
        raise
    except:
        print 'error in cfjioewcforuqirghrwrc8qolean city name for city: %s' % (city)
        city_name_clean = city
    if re.search('other|Farm House| and | road | sale |Block|House|&| in[A-Z]+', apartment_name, re.IGNORECASE):
        pass
    elif apartment_name.strip() == '' or apartment_name.strip() == '.' or '&' in apartment_name:
        pass
    else:
        try:
            solr_results = solr_fetch(apartment_name, city_name_clean)
        except KeyboardInterrupt:
            raise
        except:
            print 'some error with', (apartment_name, city_name_clean)
            solr_results=[]
        for solr_result in solr_results:
            #print solr_result
            # matching apartment name
            if re.search(solr_result['PROJECT_NAME'], apartment_name, re.IGNORECASE):
                # matching city name
                if re.search(solr_result['CITY'], city_name_clean, re.IGNORECASE):
                    # matching locality
                    if re.search(solr_result['LOCALITY'], locality, re.IGNORECASE):
                        print 'match found for', (apartment_name, locality, city), 'with', solr_result
                        #print solr_result['PROJECT_ID']
                        return solr_result['PROJECT_ID']


def crawled_data_to_process(crawl_cycle_to_process):
    query = 'select apartment_name, locality, city from crawled_data where transaction_type="Resale" and pt_project_id is null and crawl_cycle="%s" group by apartment_name , locality , city' % (
        crawl_cycle_to_process)
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


def mark_crawl_cycle_into_db(current_crawling_cycle):
    query = 'update crawled_data SET crawl_cycle="%s" where crawl_cycle is null' % (current_crawling_cycle)
    try:
        cursor.execute(query)
        db.commit()
        print 'crawl_cycle marked'
        return 'done'
    except:
        db.rollback()
        print 'marking crawl_cycle failed'
        return 'failed'


def after_crawl_stats(current_crawling_cycle):
    cursor.execute('select count(*) from crawled_data where crawl_cycle="%s"'%(current_crawling_cycle))
    total_crawled_row = cursor.fetchall()
    print total_crawled_row

    cursor.execute('select count(*) from crawled_data where crawl_cycle="%s" and ptype="Buy"'%(current_crawling_cycle))
    total_buy_type_rows = cursor.fetchall()
    print total_buy_type_rows

    cursor.execute('select count(*) from crawled_data where crawl_cycle="%s" and ptype="Rent"'%(current_crawling_cycle))
    total_rent_type_rows = cursor.fetchall()

    cursor.execute(
        'select count(*) from crawled_data where ptype="Buy" and transaction_type="Resale" and crawl_cycle="%s"'%(current_crawling_cycle))
    total_resale_buy_type = cursor.fetchall()

    cursor.execute(
        'select count(*) from crawled_data where ptype="Rent" and transaction_type="Resale" and crawl_cycle="%s"'%(current_crawling_cycle))
    total_resale_rent_type = cursor.fetchall()

    return [{'total_crawled_row': total_crawled_row}, {'total_buy_type_rows': total_buy_type_rows},
            {'total_rent_type_rows': total_rent_type_rows}, {'total_resale_buy_type': total_resale_buy_type},
            {'total_resale_rent_type': total_resale_rent_type}]

def update_lookup_table(apartment_name, locality, city, project_id, match_method):
    query='insert into lookup_table(apartment_name, locality, city, project_id, match_method) values("%s", "%s", "%s", "%s", "%s")'%(apartment_name, locality, city, project_id, match_method)
    try:
        cursor.execute(query)
        db.commit()
    except:
        db.rollback()

def not_matching_finder(current_crawl_cycle):
    cursor.execute('select apartment_name, locality, city, count(*), url from crawled_data where pt_project_id is null and crawl_cycle="%s" group by apartment_name,locality,city'%(current_crawling_cycle))
    results=cursor.fetchall()
    return results

def outliers_finder(llist):
    number_of_outliers=len(llist)/4 if len(llist)>=4 else 1
    r=pyasl.generalizedESD(numpy.array(llist), number_of_outliers, 0.45, fullOutput=True)
    return [llist[i] for i in r[1]]

def mark_outliers(project_id, outlier_rate):
    query='update crawled_data set is_outlier="yes" where pt_project_id="%s" and rate="%s" and crawl_cycle="%s" and ptype="buy" and rate!=0'%(project_id, outlier_rate, current_crawling_cycle)
    try:
        cursor.execute(query)
        db.commit()
        print outlier_rate, 'has marked as outlier'
    except:
        db.rollback()
        print 'error in marking'

def mark_non_outliers(project_id, non_outlier_rate, current_crawl_cycle):
    query='update crawled_data set is_outlier="no" where pt_project_id="%s" and rate="%s" and crawl_cycle="%s" and ptype="buy" and transaction_type="Resale" and rate!=0'%(project_id, non_outlier_rate, current_crawl_cycle)
    try:
        cursor.execute(query)
        db.commit()
    except:
        db.rollback()
        print 'error in marking non outlier', (project_id, non_outlier_rate)

def outliers_marker_in_crawled_data(current_crawl_cycle):
    # for Buy type
    count_outliers_buy=0
    cursor.execute('SET group_concat_max_len=15000')
    cursor.execute('select pt_project_id, group_concat(rate order by rate asc),city, property_type from crawled_data where pt_project_id is not null and ptype="Buy" and crawl_cycle="%s" and datediff(date(time_stamp),posted_date)<180 and transaction_type="Resale" group by pt_project_id'%(current_crawl_cycle))
    buy_resale_rows=cursor.fetchall()
    for row in buy_resale_rows:
        project_id=row[0]
        rates=[int(j) for j in row[1].split(',')]
        if len(rates)>=4:
            for outlier in outliers_finder(rates):
                count_outliers_buy+=1
                mark_outliers(project_id, outlier)

        elif len(rates)==2:
            if rates[0]<=1.2*rates[1] and rates[0]>=0.8*rates[1] and rates[1]<=1.2*rates[0] and rates[1]>=0.8*rates[0]:
                mark_non_outliers(project_id, rates[0], current_crawl_cycle)
                mark_non_outliers(project_id, rates[1], current_crawl_cycle)
            else:
                locality_price=locality_price_finder(locality_id_finder(project_id))
                if abs(locality_price-rates[0])>abs(locality_price-rates[1]):
                    mark_non_outliers(project_id, rates[1], current_crawl_cycle)
                if abs(locality_price-rates[1]>abs(locality_price-rates[0])):
                    mark_non_outliers(project_id, rates[0], current_crawl_cycle)

        elif len(rates)==3:
            ratios=[float(rates[0])/rates[1], float(rates[0])/rates[2], float(rates[1])/rates[2]]
            if ratios[0]>0.8 and ratios[1]>0.8 and ratios[2]>0.8:
                mark_non_outliers(project_id, rates[0], current_crawl_cycle)
                mark_non_outliers(project_id, rates[1], current_crawl_cycle)
                mark_non_outliers(project_id, rates[2], current_crawl_cycle)
            if ratios[0]>=0.8 and ratios[1]<0.8 and ratios[2]<0.8:
                mark_non_outliers(project_id, rates[1], current_crawl_cycle)
                mark_non_outliers(project_id, rates[2], current_crawl_cycle)
            if ratios[0]<0.8 and ratios[1]>=0.8 and ratios[2]<0.8:
                mark_non_outliers(project_id, rates[0], current_crawl_cycle)
                mark_non_outliers(project_id, rates[2], current_crawl_cycle)
            if ratios[0]<0.8 and ratios[1]<0.8 and ratios[2]>=0.8:
                mark_non_outliers(project_id, rates[0], current_crawl_cycle)
                mark_non_outliers(project_id, rates[1], current_crawl_cycle)
            if (ratios[0]>=0.8 and ratios[1]>=0.8 and ratios[2]<0.8) or (ratios[0]>=0.8 and ratios[1]<0.8 and ratios[2]>=0.8) or (ratios[0]<0.8 and ratios[1]>=0.8 and ratios[2]>=0.8):
                mark_non_outliers(project_id, rates[0], current_crawl_cycle)
                mark_non_outliers(project_id, rates[1], current_crawl_cycle)
                mark_non_outliers(project_id, rates[2], current_crawl_cycle)


    # for Rent type
    count_outliers_rent=0
    cursor.execute('SET group_concat_max_len=15000')
    cursor.execute('select pt_project_id, group_concat(rate order by rate asc),city, property_type from crawled_data where pt_project_id is not null and ptype="Rent" and crawl_cycle="%s" and datediff(date(time_stamp),posted_date)<180 and transaction_type="Resale" group by pt_project_id having count(*)>=4'%(current_crawl_cycle))
    rent_resale_rows=cursor.fetchall()
    for row in rent_resale_rows:
        project_id=row[0]
        rates=[int(j) for j in row[1].split(',')]
        for outlier in outliers_finder(rates):
            count_outliers_rent+=1
            mark_outliers(project_id, outlier)

    return {'outliers_marked_for_buy_type':count_outliers_buy, 'outliers_marked_for_rent_type':count_outliers_rent}

def non_outliers_fetcher(current_crawl_cycle, ptype):
    cursor.execute('SET group_concat_max_len=15000')
    cursor.execute('select pt_project_id, locality, city, rate, date_format(date(posted_date), "%s"), is_outlier, apartment_name from crawled_data where pt_project_id is not null and ptype="%s" and transaction_type="Resale" and crawl_cycle="%s" and datediff(date(time_stamp),posted_date)<180'%('%Y-%m-01', ptype, current_crawl_cycle))
    rows=cursor.fetchall()
    return rows

def locality_price_finder(locality_id):
    url="http://www.proptiger.com/data/v1/trend-list/current?filters=localityId==%s;unitType==Apartment&fields=wavgPricePerUnitAreaOnLtdLaunchedUnit,wavgSecondaryPricePerUnitAreaOnLtdLaunchedUnit&group=localityId"%(locality_id)
    try:
        r=requests.get(url, timeout=180)
    except Exception, e:
        print e, str(datetime.datetime.now())
        send_mail('ashok.danga@proptiger.com', 'error in API at %s'%(str(datetime.datetime.now())), str(e))
        raise
    try:
        primary_price=r.json()['data'][0]['extraAttributes']['wavgPricePerUnitAreaOnLtdLaunchedUnit']
        if primary_price == None:
            primary_price=0
    except:
        primary_price=0
    try:
        secondary_price=r.json()['data'][0]['extraAttributes']['wavgSecondaryPricePerUnitAreaOnLtdLaunchedUnit']
        if secondary_price==None:
            secondary_price=0
    except:
        secondary_price=0
    if primary_price==0 or secondary_price==0:
        return primary_price or secondary_price
    else:
        return (primary_price+secondary_price)/2.0

def locality_id_finder(project_id):
    #url='http://www.proptiger.com/app/v3/project-detail/%s?selector={"fields":["localityId"]}'%(project_id)
    url='http://www.proptiger.com/data/v1/entity/project?selector={"fields":["localityId"],"filters":{"and":[{"equal":{"projectId":%s}}]}}'%(project_id)
    r=requests.get(url)
    try:
        #locality_id=r.json()['data']['localityId']
        locality_id=r.json()['data'][0]['localityId']
    except:
        print 'error in find locality id, url is', url
        #send_mail('ashok.danga@proptiger.com', 'error in finding locality for url %s'%(url), 'error in finding locality for url %s'%(url))
        return ''
    return locality_id

def apply_rules(crawl_cycle):
    try:
        cursor.execute('update crawled_data set is_outlier ="not good" where crawl_cycle =%s and pt_project_id is not null and rate<100'%(crawl_cycle))
        db.commit()
    except:
        print 'some error in mysql'
        db.rollback()
    try:
        cursor.execute('update crawled_data set is_outlier ="yes" where crawl_cycle =%s and pt_project_id is not null and rate>100 and rate<1000 and property_type regexp "Residential Apartment"'%(crawl_cycle))
        db.commit()
    except:
        print 'some error in mysql'
        db.rollback()
    try:
        query = 'update crawled_data set is_outlier ="yes" where crawl_cycle =%s and pt_project_id is not null and rate>100000'%(crawl_cycle)
        cursor.execute(query)
        db.commit()
    except:
        print 'some error in mysql'
        db.rollback()
    try:
        query = 'update crawled_data set is_outlier ="yes" where crawl_cycle =%s and pt_project_id is not null and city not regexp "mumbai" and rate>50000 and rate<100000'%(crawl_cycle)
        cursor.execute(query)
        db.commit()
    except:
        print 'some error in mysql'
        db.rollback()

def resale_price_poster_to_CMS(PID, price, effective_date):
  url='http://noida-1.proptiger-ws.com:8888/'
  data={'EFFECTIVE_DATE':effective_date, 'PROJECT_ID':PID, 'price':price}
  headers={'content-type': 'application/json'}
  r=requests.post(url, data=json.dumps(data), headers=headers)
  #if 'failed' in r.text:
  print r.text
  return 'ok'

if __name__ == '__main__':
    current_crawling_cycle = find_last_crawl_cycle() + 1
    #current_crawling_cycle=5

    # send mail to inform that new crawl_cycle is starting
    ''' send_mail('ashok.danga@proptiger.com', 'crawl cycle number:%s has started' % (current_crawling_cycle),
              'crawl cycle number:%s has started. this script will send you email to share statistics' % (
                  current_crawling_cycle), ) '''
    
    
    '''url = 'https://www.googleapis.com/customsearch/v1?key=AIzaSyDQIDJgDaRdlRlYSuILD-zEQFlKcPr5un8&cx=002760652607811511012:t1jo4nvgipg&q=subjects'
    url = 'https://www.googleapis.com/customsearch/v1?key=AIzaSyDTKF8BMN1jUu0BIQ5M8C7oKRX81OOa_JQ&cx=014986349955534885804:xxbtmsidky8&q=m3m marina'    
    r = requests.get(url)
    print r.text '''

    

    
    
    # step 1: start all crawlers and generate CSV file of crawled data for future
    start_crawling(current_crawling_cycle)
    input_var = input("Enter something: myspider- pipelines \n")    
    # step 2: mark crawl_cycle
    crawl_cycle_marking_status = mark_crawl_cycle_into_db(current_crawling_cycle)
    if crawl_cycle_marking_status == 'done':
        send_mail('ashok.danga@proptiger.com', 'crawl cycle marking has done for cycle %s' % (current_crawling_cycle),
                  'crawl cycle marking has done for cycle %s' % (current_crawling_cycle))
        #send_mail('ashok.danga@proptiger.com', 'crawling has completed for cycle %s' % (current_crawling_cycle), str(after_crawl_stats(current_crawling_cycle)))
    else:
        send_mail('ashok.danga@proptiger.com', 'crawl cycle marking has failed for cycle %s' % (current_crawling_cycle),
                  'crawl cycle marking has failed for cycle %s' % (current_crawling_cycle))

    # step 3: fetch crawled data for matching
    rows_to_match = crawled_data_to_process(current_crawling_cycle)

    # step 4: matching with lookup table and solr; and update lookup table
    match_count_from_lookup_table=0
    match_count_from_solr=0
    #pairs_tobe_sent_lookup_table=set()
    match_count=0
    for row in rows_to_match:
        match_count+=1
        matching_result = matcher_from_lookup_table(row[0], row[1], row[2])
        if matching_result != ():
            match_count_from_lookup_table+=1
            try:
                push_found_pid_to_crawled_data(row[0], row[1], row[2], int(float(matching_result[0][0])), current_crawling_cycle)
            except:
                print 'some error while pushing PID to DB:'

        else:
            # match with solr
            matching_result_from_solr=solr_matcher(row[0], row[1], row[2])
            if matching_result_from_solr:
                match_count_from_solr+=1
                push_found_pid_to_crawled_data(row[0], row[1], row[2], matching_result_from_solr, current_crawling_cycle)
                update_lookup_table(row[0], row[1], row[2], matching_result_from_solr, 'solr')
                #pairs_tobe_sent_lookup_table.add((row[0], row[1], row[2], matching_result_from_solr))
        print 'match_count:', match_count

    # now push to update lookup table
    #for i in pairs_tobe_sent_lookup_table:
        #update_lookup_table(i[0], i[1], i[2], i[3], 'solr')

    after_matching_stats=[{'Total':len(rows_to_match)}, {'lookup_table_match_count':match_count_from_lookup_table}, {'solr_match_count':match_count_from_solr}]
    send_mail('ashok.danga@proptiger.com', 'After crawling matching statistics for crawl_cycle: %s'%(current_crawling_cycle), str(after_matching_stats))

    # step 5: send unmatched to suneel
    unmatched_rows=not_matching_finder(current_crawling_cycle)
    file_name=open('unique_apartments_unmatched_cycle_%s.csv'%(current_crawling_cycle), 'w')
    writer=csv.writer(file_name)
    writer.writerow(['apartment_name','locality','city','count','url'])
    for row in unmatched_rows:
        writer.writerow(row)
    file_name.close()
    subprocess.call(['gzip', '-f', 'unique_apartments_unmatched_cycle_%s.csv'%(current_crawling_cycle)])
    send_mail('ashok.danga@proptiger.com', 'unique apartments not matching for cycle %s'%(current_crawling_cycle), 'unique apartments not matching for cycle %s. CSV file is attched'%(current_crawling_cycle), 'unique_apartments_unmatched_cycle_%s.csv.gz'%(current_crawling_cycle))
    #send_mail('suneel.kumar@proptiger.com', 'unique apartments not matching for cycle %s'%(current_crawling_cycle), 'unique apartments not matching for cycle %s. CSV file is attched'%(current_crawling_cycle), 'unique_apartments_unmatched_cycle_%s.csv'%(current_crawling_cycle))


    # step 6: mark outliers
    outliers_marking_status=outliers_marker_in_crawled_data(current_crawling_cycle)
    if outliers_marking_status:
        send_mail('ashok.danga@proptiger.com', 'outliers have marked for crawl_cycle: %s'%(current_crawling_cycle), str(outliers_marking_status))


    # step 7: applying rules2.0 and creating final data tobe sent to CMS
    # buy type data
    apply_rules(current_crawling_cycle)

    # Rent type data
    # will add in future

    # step 8: create files for review and send data to CMS
    query = 'select apartment_name , locality , city, pt_project_id , rate, date_format(date(posted_date), "%Y-%m-01")'+' from crawled_data where pt_project_id is not null and crawl_cycle =%s and transaction_type="Resale" and (is_outlier="no" or is_outlier is null)'%(current_crawling_cycle)
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        print 'alok:', len(rows)
    f1 = open('final_price_to_CMS_cycle_%s.csv'%(current_crawling_cycle), 'w')
    writer1 = csv.writer(f1)
    f2 = open('highlighted_price_cycle_%s.csv'%(current_crawling_cycle), 'w')
    writer2 = csv.writer(f2)
    count_locality_price_not_found=0
    count_locality_price_not_in_range=0
    count_good=0

    for idx, row in enumerate(rows):
        locality_price = locality_price_finder(locality_id_finder(row[3]))

        if locality_price:
            if int(row[4])>=locality_price*0.5 and int(row[4])<=locality_price*2:
                count_good+=1
                writer1.writerow(row)
            else:
                count_licality_price_not_in_range+=1
                writer2.writerow(row)
        else:
            count_licality_price_not_found=count_licality_price_not_found+1
            writer2.writerow(row)

        print idx, row
        print locality_price
        print 'count_good', count_good
        print 'count_licality_price_not_found', count_licality_price_not_found
        print 'count_licality_price_not_in_range', count_licality_price_not_in_range











