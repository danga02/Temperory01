# Scrapy settings for mycrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'mycrawler'

SPIDER_MODULES = ['mycrawler.spiders']
NEWSPIDER_MODULE = 'mycrawler.spiders'
ITEM_PIPELINES = ['mycrawler.pipelines.mycrawlerSqlPipeline']

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'mycrawler (+http://www.yourdomain.com)'
USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0'
#LOG_LEVEL = 'INFO'
REDIRECT_ENABLED = False

MYSQLDB_CONNARGS = (
    "ec2-54-255-51-21.ap-southeast-1.compute.amazonaws.com",
    "root",
    "proptiger",
    "crawling"
)
