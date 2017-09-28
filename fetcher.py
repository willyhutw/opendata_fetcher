
# -*- coding: utf-8 -*-

import os
import csv
import yaml
import glob
import json
import logging
import threading
from zipfile import ZipFile
from configparser import ConfigParser
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.client import KafkaClient as KClient
from kafka.errors import KafkaError

config = ConfigParser()
config.read('fetcher.conf')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def topic_exist(logger, topic):
    result = False
    try:
        consumer = KafkaConsumer(bootstrap_servers=[config['default']['kafka_url']])
        if topic in consumer.topics():
            result = True
    except Exception as e:
        logger.error("===check_topic")
        logger.error(e)
    return result


def create_topic(logger, topic):
    result = False
    try:
        producer = KafkaProducer(bootstrap_servers=[config['default']['kafka_url']])
        future = producer.send('my-topic', b'hello world')
        record_metadata = future.get(timeout=10)
    except Exception as e:
        logger.error("===create_topic")
        logger.error(e)
    return result


def unzip_files(logger, target_dir):
    logger.info("=== Unzip data files in " + target_dir)
    pwd = os.getcwd()
    for re_path in glob.glob(target_dir + '/*.zip'):
        with ZipFile(re_path, 'r') as zip_file:
            logger.info("=== Unzipping file : " + re_path)
            zip_file.extractall(pwd + '/' + re_path[:-4])


def list_files(logger, target_dir):
    files = []
    for d in os.listdir(target_dir):
        if os.path.isdir(target_dir + '/' + d):
            for re_path in glob.glob(target_dir + '/' + d +'/[A-Za-z]_*_*_[A-Za-z].CSV'):
                files.append(re_path)
    logger.info("=== Raw files in " + target_dir)
    logger.info(files)
    return files


def produce_messages(re_path, codec, topic):
    producer = KafkaProducer(bootstrap_servers=[config['default']['kafka_url']])
    with open(re_path, encoding=codec, errors='replace') as csvfile:
        csv_rows = csv.reader(csvfile, delimiter=',')
        header = next(csv_rows)
        for row in csv_rows:
            msg = {}
            for h, v in zip(header, row):
                msg[h] = v
            producer.send(topic, json.dumps(msg).encode('utf-8'))


def worker(conf):
    topic = conf[0]["TOPIC"]
    data_path = conf[1]["DATA_PATH"]
    codec = conf[2]["CODEC"]

    # setup logger
    logger = setup_logger(topic, 'data/' + topic + '.log')
    logger.info('starting %s worker...' % topic)
    logger.debug(conf)
    
    # check topic
    #if not topic_exist(logger, topic):
    #    create_topic(logger, topic)
    unzip_files(logger, data_path)
    files = list_files(logger, data_path)
    for f in files:
        produce_messages(f, codec, topic)



def main():
    # loop units
    for unit in config['default']['units'].splitlines():
        if unit:
            try:
                threads = []
                with open(config['default']['units_path'] + '/' + unit, 'r') as unit_file:
                    unit_conf = yaml.load(unit_file.read())
                    t = threading.Thread(target=worker, args=(unit_conf,))
                    threads.append(t)
                    t.start()
            except Exception as e:
                print(e)

if __name__ == "__main__":
    main()

### check kafka topic

### parsing raw data

### send message to kafka


