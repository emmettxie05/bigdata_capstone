# store criminal record to kafka
#


import argparse
import atexit
import logging
import csv
import json
import ConfigParser

from kafka import KafkaProducer
from kafka.errors import KafkaError
from apscheduler.schedulers.background import BackgroundScheduler

kafka_broker = ''

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG);

#schedule = BackgroundScheduler()
#schedule.add_excutor('threadpool')
#schedule.start()

config=ConfigParser.ConfigParser()
config.read('crime_data.config')

csv_file_2017 = config.get('input', 'crime_file_2017')
csv_file_2016 = config.get('input', 'crime_file_2016')
#print (csv_file_2017)
# load csv file to kafka

def shutdown_hook(producer):
    try:
        logger.info("begin to flush message in 10s")
        producer.flush(10)
    except kafkaError as kafka_error:
        logger.warn("Failed to flush message")
    finally:
        try:
            logger.info('close kafka producer connection')
            producer.close()
        except kafkaError as kafka_error:
            logger.warn("Failed to close kafka connection")


def load_csv(producer, csv_file):
    try:
        logger.debug('begin to read csv file')
        with open(csv_file, 'rb') as csvfile:
           crime_reader = csv.DictReader(csvfile)
           for row in crime_reader:
 #              print(row)
               producer.send('meta_crime_data', value=json.dumps(row).encode('utf-8'))
    except KafkaError as ke:
        logger.warn('failed to send data to kafka')               


if __name__ == '__main__':
    #logger.debug("Begin to write data to kafka.")
    #sc =  SparkContext
    parser = argparse.ArgumentParser()
    parser.add_argument('kafka_broker', help='the location of kafka broker')

    args = parser.parse_args()
    kafka_broker = args.kafka_broker

    print(kafka_broker)
    
    # create kafka server
    producer = KafkaProducer(bootstrap_servers = kafka_broker)

    load_csv(producer, csv_file_2017)

    atexit.register(shutdown_hook, producer)

    
