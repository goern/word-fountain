import argparse
import locale
import os
import random
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Kafka word fountain')
parser.add_argument('--server', help='A bootstrap server', default='localhost:9092')
parser.add_argument('--topic', help='Topic to publish to', default='tmp')
parser.add_argument('--rate', type=int, help='Words per second', default=10)
parser.add_argument('--count', type=int, help='Total words to publish', default=101)
args = parser.parse_args()

servers = [os.getenv('SERVER', args.server)]
topic = os.getenv('TOPIC', args.topic)
rate = int(os.getenv('RATE', args.rate))
count = int(os.getenv('COUNT', args.count))

print('servers={}, topic={}, rate={}, count={}'.format(servers, topic, rate, count))

producer = KafkaProducer(bootstrap_servers=servers)

with open('/usr/share/dict/words') as f:
    words = [bytes(w.strip(), locale.getpreferredencoding(False))
             for w in f.readlines()]

while count:
    producer.send(topic, random.choice(words))
    count -= 1
#    if not count % (rate * 5):
#        print(producer.metrics())
    time.sleep(1 / rate)

