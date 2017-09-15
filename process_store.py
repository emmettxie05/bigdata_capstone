# get data from kafka, and store it to cassandra


import argparse
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import logging
import atexit


kafka_broker = ''
kafka_topic = ''
cassandra_broker = ''
keyspace = '' 
table = ''


logging.basicConfig(format=logger_format)
logger = logging.getLogger('process_store')
logger.setLevel(logger.DEBUG)

def shutdown_hook(session, consumer):
    try:
        logger.info("begin to close kafka consumer")
        consumer.close()
        session_shutdown()
    except kafkaError as kafka_error:
        logger.warn("failed to close kafka consumer")
    finally:
        logger.info("kafka consumer is closed")
        try:
            logger.info("begin to close cassandra")
            consumer.close()
        except Exception as e:
            logger.info("failed to close cassandra session")
        finally:
            logger.info("All services are closed")

def extract_date(crime_data, session):
    logger.debug("extract %s data" % crime_data)
    parsed = json.loads(crime_data)[0]
    incid = parsed.get('IncidntNum')
    cate = parsed.get('DRUG/NARCOTIC')
    address = parsed.get('Address')
    data = parsed.get('Date')
    time = parsed.get('Time')
    DOW = parsed.get('DayOfWeek')
    pd = parsed.get('PdDistrict')
    pdid = parsed.get('PdId')
    result = parsed.get('Resolution')
    try:
        logger.debug("insert data to table")
        statement = "INSERT INTO %s (incident_id, catagory, address, data, pd, pdid, soultion) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (incid,cate,address,data,pd,pdid,result)
        session.execute(statement)
    except Exception as e:
        logger.error("failed to insert data")



if __name__ = '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('kafka_broker', help=' the position of kafka broker')
    parser.add_argument('cassandra_broker', help=' the contact points fo cassandra')
    parser.add_argument('keyspace', help=' the name of keyspace')
    parser.add_argument('table', help='the name of table')

    args = parser.parse_args()

    kafka_broker = args.kafka_broker
    kafka_topic = args.kafka_topic
    cassandra_broker = args.cassandra_broker
    keyspace = args.keyspace
    table = args.table


    # start kafka consumer
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_broker)
    # start cassandra session
    cassandra_connection = Cluster(contact_points=cassandra_broker.split(','))

    session = cassandra_connection.connect()

    statment_create_ks = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % keyspace
    session.execute(statment_create_ks)

    session.set_keyspace(keyspace)

    statment_create_tab ="CREATE TABLE IF NOT EXISTS %s (incident_id text, catagory text, address text, data , pd text, pdid test, solution test, PRIMARY KEY(incident_id, pdid))" % table
    session.execute(statment_create_tab)

    atexit.register(shutdown_hook, session, consumer)

    for msg in consumer:
        extract_data(msg.value, session)



