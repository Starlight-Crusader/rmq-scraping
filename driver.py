import threading
from rmq import Publisher
from crawler import Crawler
import os
from settings import Settings

os.system("clear")

try:
    os.remove("db.json")
except:
    pass

# Push the starting url to the queue
Publisher.send('urls', Settings.STARTING_URL)

# Create a list of consumer instances
consumers = [Crawler(i) for i in range(Settings.NUM_OF_CONSUMERS)]
Crawler.max_pages = Settings.MAX_PAGES

# Start each consumer in a separate thread
threads = [threading.Thread(target=consumer.process, args=('urls',)) for consumer in consumers]

# Start the threads
for thread in threads:
    thread.start()

# Wait for all threads to end
for thread in threads:
    thread.join()