import MySQLdb



conn1 = MySQLdb.connect(host='192.168.1.1', user='root', passwd='root', db='cms')
conn2 = MySQLdb.connect(host='localhost', user='root', passwd='proptiger', db='crawling')

