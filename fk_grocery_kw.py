import sys
import os
import re
import pytz
import datetime
import scrapy
import json
import pymysql
import requests
import random
import time

import mysql.connector

from scrapy.selector import Selector
from scrapy.loader import ItemLoader
from datetime import datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.http import HtmlResponse
from w3lib.html import remove_tags
from slugify import slugify
from itertools import repeat
from scrapy.spiders.init import InitSpider
from utils import utils
from db.db_action import DBAction

from bs4 import BeautifulSoup as bs

# Addons (For Now)
from .addons import Get, Post, Session

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

SETTINGS = scrapy.utils.project.get_project_settings()

class FKGROWebsiteKWSpider(InitSpider):
    name = "fk_grocery_kw"
    
    allowed_domains = ["flipkart.com", "1.rome.api.flipkart.com"]
    start_urls = ["https://www.flipkart.com/"]
    custom_settings = {
            # specifies exported fields and order
            "DOWNLOAD_TIMEOUT": 60000,
            "DOWNLOAD_MAXSIZE": 12406585060,
            "DOWNLOAD_WARNSIZE": 0,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN":1,
            "CONCURRENT_REQUESTS_PER_IP":1,
            "HTTPPROXY_ENABLED": True,
            "DOWNLOADER_MIDDLEWARES" : {
                "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
                "scrapy_web_crawlers.middlewares.RandomUserAgentMiddleware": 1,
                },
        }    
    HOME_URL = "https://www.flipkart.com/grocery-supermart-store?marketplace=GROCERY"
    SEARCH_URL = "https://www.flipkart.com/search?q={}&marketplace=GROCERY&as-show=on&as=off"
    
    def __init__(self, inputs, **kw): # pf_id=None, location_id=None, location=None, pincode=None, brand_id=None, brand_name=None, keyword_id=None, keyword=None, page=1, location_search="0",crawl_id=None, **kw):
        super(FKGROWebsiteKWSpider, self).__init__(**kw)

        print("PF ID: {0} ".format(inputs.get("pf_id")))
        print("KEYWORD ID: {0} ".format(inputs.get("keyword_id")))
        print("KEYWORD:  {0} ".format(inputs.get("keyword")))
        print("LOCATION:  {0} ".format(inputs.get("location")))
        print("LOCATION ID: {0} ".format(inputs.get("location_id")))
        print("PINCODE:  {0} ".format(inputs.get("pincode")))
        print("PAGE: {0} ".format(inputs.get("page")))
        print("Site: {0} ".format(self.name))

        self.env = inputs.get("env")
        self.site = self.name
        self.db_name = inputs.get("db_name")
        self.crawl_id = int(inputs.get("crawl_id"))
        self.pf_id = int(inputs.get("pf_id"))        
        self.location_id = int(inputs.get("location_id"))
        self.location = inputs.get("location")
        self.pincode = str(inputs.get("pincode")) 
        self.keyword = inputs.get("keyword")
        self.keyword_id = int(inputs.get("keyword_id"))
        self.brand_id = int(inputs.get("brand_id"))
        self.brand_name = inputs.get("brand_name")
        self.page = int(inputs.get("page"))
        self.location_search = inputs.get("location_search")
        self.sponsored_index = 0
        self.index = 0
        
        self.tz = inputs.get("tz")
        self.current_time = inputs.get("current_time")
        self.dt = inputs.get("dt")
        self.base_dir = "C:/var/logs/"+self.site
        self.output_file = self.base_dir+"/output/out_"+self.pincode+"_"+self.keyword+".txt"

        self.db_table_name = "flipkart_crawl_kw"       
        
        self.output_cols = [
                                "pf_id","crawl_id","keyword","keyword_id","brand_id","brand_name","brand_crawl","web_pid","pdp_title_value",
                                "position","price_sp","pdp_rating_value","pdp_rating_count","pdp_image_url","pdp_page_url","pdp_discount_value",
                                "pdp_sponsored","sponsored_brand","location_id","location_name","pincode","created_by","created_on","status","page"
                            ]
        
        self.visit_count = str(random.choice([10,11,12,13,14,15,16]))
        
        self.session = Session()
        self.db_action = DBAction()
        try:
            self.connection = self.db_action.db_connection(db_name=self.db_name, env=self.env)
            self.cursor = self.connection.cursor()
        except mysql.connector.Error as error:
            self.logger.error("Connection error with the database Error:{}:{}".format(error.errno, error.strerror))
            print("Connection error with the database Error{}:{}".format(error.errno, error.strerror))

    def init_request(self):    
        #Home Page Request
        # self.logger.info("Home Page Request")
        for page in range(1, int(self.page)+1):
            payload = {"locationContext": {"pincode": self.pincode}, "marketplaceContext": {"marketplace": "GROCERY"}}
            x = Post(self.session,"https://1.rome.api.flipkart.com/api/3/marketplace/serviceability",data = payload)
            header = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Access-Control-Request-Headers': 'content-type,x-user-agent',
                'Access-Control-Request-Method': 'POST',
                'Connection': 'keep-alive',
                'Origin': 'https://www.flipkart.com',
                'Referer': 'https://www.flipkart.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
            }
            
            keyword_res = self.keyword_search(page=page)
            # keyword_res=requests.options('https://1.rome.api.flipkart.com/api/3/marketplace/serviceability', headers=headers)
            if keyword_res and keyword_res.status_code < 400:
                all_items = self.parse_list_page(response=keyword_res, keyword=self.keyword, page=page)
                print('FLIPKART_GROCERY_KEYWORD', len(tuple(all_items)), page, self.keyword)
            else:
                if keyword_res:
                    self.logger.error("Keyword Page error: (or Search page empty) | StatusCode: " + str(keyword_res.status_code))



    def keyword_search(self, page):
        #for crawl_input in self.crawl_inputs:
        keyword = self.keyword
        self.logger.info("Crawl Keyword : "+keyword)

        url = self.SEARCH_URL.format(keyword.strip())
        if page and int(page)-1:
            url += f'&page={page}'
        return Get(session=self.session, url=url, sleep=1)

    def parse_list_page(self, response, keyword, page):
        items = list()

        self.logger.info("Got successful response from {}".format(response.url))
        self.logger.info(response.request.headers)
        
        webpage_dir = "%s/webpages/%s" % (self.base_dir, str(self.dt))
        filename = "/%s-%s-%s-%s.html" % (str(page), str(self.pincode), self.site, keyword)
        utils.save_source_content(webpage_dir, filename, response.content)
        
        self.logger.info("Saved file %s" % filename)
        self.logger.info("Data Extraction Start")
        
        # script = response.xpath("//script[@id='is_script']/text()").get()
        response_selector = bs(response.text, 'html.parser')
        script = response_selector.find('script', {'id': 'is_script'}).decode_contents()

        if script is not None:
            json_response = json.loads(script.replace("window.__INITIAL_STATE__ = ","").replace(";","").strip())            
            if '"pageData":' in script:
                data = json_response["pageDataV4"]["page"]["data"]
                browse_meta_data = json_response["pageDataV4"]["browseMetadata"]
                sqid = browse_meta_data["sqid"]
                ssid = browse_meta_data["ssid"]

                page_data = json_response["pageDataV4"]["page"]["pageData"]
                pagination_context_map = page_data["paginationContextMap"]
                pagination_context_map = str(pagination_context_map).replace("'","\"")
                sections = list()
                for key in data.keys():
                    data_array = data[str(key)]
                    for sec in data_array:
                        sections.append(sec)
                product_detail = None
                product_specification = None
                physical_attach = None
                multimedia = None
                qna = None
                products = list()
                for section in sections:
                    try:
                        if "PRODUCT_SUMMARY" in section["elementId"]:
                            for product in section["widget"]["data"]["products"]:
                                products.append(product)
                        else:
                            section=section
                    except (TypeError, AttributeError, ValueError, KeyError) as ke:
                        continue
                for product in products: 
                    item = dict()
                    product_info = product["productInfo"]["value"]
                    tracking = product["productInfo"]["tracking"]
                    rank = tracking["position"]
                    web_pid = product_info["id"]
                    item_id = product_info["itemId"]
                    try:
                        sponsored = product["adInfo"]
                        if sponsored is not None:
                            sponsored = "1"
                        else:
                            sponsored = "0"
                    except (TypeError, AttributeError, ValueError, KeyError) as ke:
                        sponsored = "0"
                    try:
                        listing_id = product_info["listingId"]
                    except (TypeError, AttributeError, ValueError, KeyError) as ke:
                        listing_id = ""

                    in_stock = product_info["availability"]["displayState"]
                    if "in_stock" in in_stock.lower():
                        vosa = 1
                        vosa_remark = "IN STOCK"
                    else:
                        vosa = 0
                        vosa_remark = "OOS"

                    title = product_info["titles"]["title"]
                    brand_name = product_info["productBrand"]

                    discount = 0
                    # Variants
                    # 1st - contains sp, mrp | 2nd contains no price related info (currently unavailable)

                    # 1st variant
                    if 'pricing' in product_info:
                        try:
                            # FK GROCERY VERSION
                            prices = product_info["pricing"]["prices"]
                            mrp = prices[0]['value']
                            # Discount varies sometimes (pdp url value differs)
                            # if "discount" in prices[0]:
                            #     discount = prices[0]['discount']
                        except:
                            # FK original
                            mrp = product_info["pricing"]["mrp"]["value"]
                        price = product_info["pricing"]["finalPrice"]["value"]
                        saving = mrp-price
                        try:
                            if not discount:
                                discount = round((saving/mrp)*100,2)
                        except:
                            discount = 0
                        # discount = product_info["pricing"]["discountAmount"]
                        currency = product_info["pricing"]["finalPrice"]["currency"]
                    else:
                        mrp = 0
                        price = 0
                        saving = 0
                        discount = 0
                        currency = "INR"

                    # Specification is not being used anywhere or stored in any column either
                    # try:
                    #     specification = product_info["keySpecs"]
                    # except Exception as e:
                    #     print('ERR_GROCERY_KW', e, title, brand_name, response.url)

                    images = product_info["media"]["images"]
                    image_url = images[0]["url"]
                    image_url = image_url.replace("{@height}","612").replace("{@width}","612").replace("{@quality}","70")
                    rating = product_info["rating"]["average"]
                    review = product_info["rating"]["count"]

                    pdp_page_url = "https://www.flipkart.com" + product_info["baseUrl"] + "&marketplace=GROCERY"

                    if sponsored == "1":
                        self.sponsored_index += 1
                        rank = self.sponsored_index
                    else:
                        self.index += 1
                        rank = self.index

                    item["pf_id"] = self.pf_id
                    item["crawl_id"] = self.crawl_id
                    item["page"] = page
                    item["keyword"] = self.keyword
                    item["keyword_id"] = self.keyword_id            
                    item["brand_id"] = self.brand_id
                    item["brand_name"] = self.brand_name
                    item["brand_crawl"] = brand_name
                    item["web_pid"] = web_pid if web_pid else ""
                    item["pdp_title_value"] = title if title else ""
                    item["position"] = rank
                    item["price_sp"] = price if price else 0
                    item["price_rp"] = mrp if mrp else 0
                    item["pdp_rating_value"] = rating if rating else 0
                    item["pdp_rating_count"] = review if review else 0
                    item["pdp_image_url"] = image_url if image_url else ""
                    item["pdp_page_url"] = pdp_page_url
                    item["pdp_discount_value"] = discount if discount else 0.0
                    item["pdp_sponsored"] = sponsored
                    item["sponsored_brand"] = brand_name  
                    item["location_id"] = self.location_id
                    item["location_name"] = self.location
                    item["pincode"] = self.pincode
                    item["created_by"] = "System"
                    item["created_on"] = self.current_time
                    item["status"] = 1
                    #item["html_index"] = index if index else 0
                    item["price_mrp"] = price if price else 0
                    #item["saving"] = saving if saving else 0
                    #item["discount"] = discount if discount else 0
                    #item["prime_pantry"] = prime if prime else ""
                    #item["pdp_image_index"] = image_index if image_index else ""   

                    item = utils.clean_item_data(item)

                    marketplace = "FLIPKART"
                    items.append(item)
            else:
                item = dict()
                item["pf_id"] = self.pf_id
                item["crawl_id"] = self.crawl_id
                item["page"] = page
                item["keyword"] = self.keyword
                item["keyword_id"] = self.keyword_id            
                item["brand_id"] = self.brand_id
                item["brand_name"] = self.brand_name
                item["brand_crawl"] = ""
                item["web_pid"] = ""
                item["pdp_title_value"] = ""
                item["position"] = 0
                item["price_sp"] = 0
                item["price_rp"] = 0
                item["pdp_rating_value"] = 0
                item["pdp_rating_count"] = 0
                item["pdp_image_url"] = ""
                item["pdp_page_url"] = ""
                item["pdp_discount_value"] = 0.0
                item["pdp_sponsored"] = 0
                item["sponsored_brand"] = ""   
                item["location_id"] = self.location_id
                item["location_name"] = self.location
                item["pincode"] = self.pincode
                item["created_by"] = "System"
                item["created_on"] = self.current_time
                item["status"] = 1
                items.append(item)

        else:
            item = dict()
            item["pf_id"] = self.pf_id
            item["crawl_id"] = self.crawl_id
            item["page"] = page
            item["keyword"] = self.keyword
            item["keyword_id"] = self.keyword_id            
            item["brand_id"] = self.brand_id
            item["brand_name"] = self.brand_name
            item["brand_crawl"] = ""
            item["web_pid"] = ""
            item["pdp_title_value"] = ""
            item["position"] = 0
            item["price_sp"] = 0
            item["price_rp"] = 0
            item["pdp_rating_value"] = 0
            item["pdp_rating_count"] = 0
            item["pdp_image_url"] = ""
            item["pdp_page_url"] = ""
            item["pdp_discount_value"] = 0.0
            item["pdp_sponsored"] = 0
            item["sponsored_brand"] = ""   
            item["location_id"] = self.location_id
            item["location_name"] = self.location
            item["pincode"] = self.pincode
            item["created_by"] = "System"
            item["created_on"] = self.current_time
            item["status"] = 1
            items.append(item)

        if items:
            records = []
            columns = self.output_cols                
            for item in items:
                # Create Raw file without header
                utils.create_raw_file(self.output_file, item.keys(), item)
                # Converting into list of tuple
                record = tuple(item.get(col, "0") for col in columns if col in item) 
                records.append(record)
            # records = [tuple(item.values()) for item in items]
            
            # Create Header file
            item = items[0]
            output_dir = self.base_dir+"/output"
            output_filename = "header.txt"
            output_file = output_dir+"/"+output_filename
            header_row = "|".join(item.keys()) +"\r"
            utils.create_file(output_file, header_row)

            self.logger.info("Data Inserting")
            tbl_columns_str = "`"+"`,`".join(self.output_cols)+"`"        
            values_list = []
            values_list.extend(repeat("%s", len(self.output_cols)))
            tbl_columns_val = ",".join(values_list)
            mySql_insert_query = """INSERT INTO `flipkart_groceries_crawl_kw` (""" +tbl_columns_str+ """) VALUES (""" +tbl_columns_val+ """)"""
            try:
                self.cursor.executemany(mySql_insert_query, records)
                products_count = self.cursor.rowcount
                self.connection.commit()
            except self.cursor.Error as err:
                self.logger.error("Database Error: "+str(err))
        return items