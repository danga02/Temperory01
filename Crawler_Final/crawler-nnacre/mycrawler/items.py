# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class MycrawlerItem(Item):
    # define the fields for your item here like:
    # name = Field(default="")
    #x = input("input here items !!")
    property_type=Field(default="")
    Apartment_name=Field(default="")
    locality=Field(default="")
    city=Field(default="")
    address=Field(default="")
    Floor_number=Field(default="")
    Possession=Field(default="")
    Property_age=Field(default="")
    Transaction_Type=Field(default="")
    Property_Ownership=Field(default="")
    bathroom = Field(default = "")

    area=Field(default="")
    area_unit=Field(default="")
    price=Field(default="")
    rate=Field(default="")
    contact_person=Field(default="")
    contact_number=Field(default="")
    url=Field(default="")
    url_referer=Field(default="")
    
    posted_date=Field(default="")
    posted_by=Field(default="")
    property_code=Field(default="")
    BHK=Field(default="")
    ptype=Field(default="")
    
    owner_type=Field(default="")
    owner_address=Field(default="")
    
    crawl_cycle=Field(default="")
    time_stamp=Field(default="")
    pt_project_id=Field(default="")
    pt_type_id=Field(default="")
    pt_project_id=Field(default="")
    is_outlier=Field(default="")
    to_CMS=Field(default="")