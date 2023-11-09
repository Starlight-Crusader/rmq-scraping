import requests, re, urllib.parse, pika
from bs4 import BeautifulSoup
from rmq import Publisher, Consumer
from tinydb import TinyDB
from db import DB
from settings import Settings
from time import sleep

ITEM_BASE = "https://999.md"


class Crawler(Consumer):
    max_pages = None
    id = None

    def __init__(self, id):
        self.id = id
    
    def normalize_chars(self, input_string, space_rep):
        translation_table = str.maketrans("ăĂțȚșȘîÎâÂ", "aAtTsSiIaA")

        normalized_string = input_string.strip()
        normalized_string = normalized_string.replace(" ", space_rep)   
        normalized_string = normalized_string.replace("   ", " ")
        normalized_string = normalized_string.replace("  ", " ")
        normalized_string = normalized_string.translate(translation_table)
        normalized_string = re.sub(r'\u00b3', '3', normalized_string)
        normalized_string = re.sub(r'\u20ac', 'e', normalized_string)
        normalized_string = normalized_string.lower()

        return normalized_string

    def scrap_catalog(self, url):
        catalog_base = url.decode('utf-8')[:url.decode('utf-8').index("?page=")+6]

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        items = soup.find(class_='items__list')
    
        for item in items.find_all(class_='ads-list-photo-item'):
            try:
                link = item.find('a')['href']
            except:
                continue
            
            if link.startswith('/booster'):
                continue
            else:
                Publisher.send('urls', urllib.parse.urljoin(ITEM_BASE, link))
    
        paginator = soup.find(class_='paginator')
        ul = paginator.find('ul')
    
        current = int(ul.find(class_='current').text)
        
        next = 0
        for li in ul.find_all('li'):
            if int(li.find('a').text) > current:
                next = int(li.find('a').text)
                break
            
        Crawler.max_pages -= 1
    
        if next != 0 and Crawler.max_pages > 0:
            Publisher.send('urls', catalog_base + str(next))
        else:
            print(f"[!] {current} page is the last one")

            for i in range(Settings.NUM_OF_CONSUMERS):
                Publisher.send('urls', "TERMINATE")
        
    def scrap_item(self, url):
        item = {}

        item['url'] = url.decode('utf-8')

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Save price

        item['price'] = self.normalize_chars(soup.find(class_='adPage__content__price-feature__prices__price__value').text, ' ')

        currency = ""

        try:
            currency = self.normalize_chars(soup.find(class_='adPage__content__price-feature__prices__price__currency').text, ' ')
        except:
            pass

        item['price'] += currency

        # Save region

        item['region'] = ""

        region_elements = soup.find(class_='adPage__content__region')
        for dd in region_elements.find_all('dd'):
            item['region'] += dd.text

        item['region'] = self.normalize_chars(item['region'], ' ')

        # Save features per groups

        features = soup.find(class_='adPage__content__features')

        for col in features.find_all(class_='adPage__content__features__col'):
            uls = col.find_all('ul')
            h2s = col.find_all('h2')

            for i in range(len(uls)):
                # Value - array
                if self.normalize_chars(h2s[i].text, "") in ["securitate", "confort"]:
                    properties = []

                    for li in uls[i].find_all('li'):
                        span_element = li.find('span')
                        properties.append(self.normalize_chars(span_element.text, " "))

                    item[self.normalize_chars(h2s[i].text, "_")] = properties
                # Value - dictionary
                else:
                    properties = {}

                    for li in uls[i].find_all('li'):
                        span_elements = li.find_all('span')

                        value = ""
                        try:
                            value = span_elements[1].find('a').text
                        except:
                            value = span_elements[1].text
                    
                        key = span_elements[0].text

                        properties[self.normalize_chars(key, '_')] = self.normalize_chars(value, ' ')

                    item[self.normalize_chars(h2s[i].text, "_")] = properties

        while not DB.available:
            sleep(0.1)

        DB.available = False
        db = TinyDB(DB.path, indent=4)
        db.insert(item)
        db.close()
        DB.available = True

        print(f"[<] C{self.id} . {item['url']}: {self.normalize_chars(soup.find(class_='adPage__header').find('h1').text, ' ')} ~ {item['price']}")

    def process(self, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=queue_name)

        def callback(ch, method, properties, body):
            if body.decode('utf-8') == "TERMINATE":
                channel.stop_consuming()
            elif "page" in body.decode('utf-8'):
                self.scrap_catalog(body)
            else:
                self.scrap_item(body)

        channel.basic_consume(
            queue=queue_name,
            auto_ack=True,
            on_message_callback=callback
        )

        print(f'[-] C{self.id} is waiting for messages. To exit press CTRL+C')
    
        channel.start_consuming()

        channel.close()
        connection.close()
