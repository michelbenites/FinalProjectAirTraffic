#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 04/30/2018
# Descr. : Getting information from Satori website and send to Kafka and Elasticsearch

from __future__ import print_function
import threading
import sys
import time
import json
import datetime
import pandas as pd
# Import Kafka producer.
from kafka import KafkaProducer
# Import Elasticsearch.
from elasticsearch import Elasticsearch
# Import Satori client and subscription.
from satori.rtm.client import make_client, SubscriptionMode

# Get the filter parameters.
sfilter  = sys.argv[1]
sflight  = sys.argv[2]

# Set the producer for Kafka.
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set elasticsearch connection.
es = Elasticsearch([{'host': '35.201.227.160', 'port': 9200}])

# Set the variables to connect satori website.
endpoint = "wss://open-data.api.satori.com"
appkey = "73eF3D8DF0c428eb7a44e6D14EbAAd9a"
channel = "air-traffic"

# Get infromation regarding to airline names
p_airlines = pd.read_csv("file:///home/min257/database/airlinecodes.csv", sep=";", encoding="utf-8", index_col="code")

# Main function.
def main():

    # Function to insert data on Elasticsearch index.
    def insert_data_es(dict_tmp):

        # Try to verify if everything is ok.
        try:
            # Create a empty dictionary python variable.
            d_doc    = {}
            # Fill the variable only if it is not empty.
            if bool(dict_tmp):
                d_doc['code']         = dict_tmp['code']
                d_doc['airline']      = dict_tmp['airline']
                d_doc['flight']       = dict_tmp['flight']
                d_doc['aircraft']     = dict_tmp['aircraft']
                d_doc['origin']       = dict_tmp['origin']
                d_doc['destination']  = dict_tmp['destination']
                d_doc['callsign']     = dict_tmp['callsign']
                d_doc['registration'] = dict_tmp['registration']
                d_doc['speed']        = dict_tmp['speed']
                d_doc['altitude']     = dict_tmp['altitude']
                d_doc['location']     = dict_tmp['location']
                d_doc['datetime']     = dict_tmp['datetime']
                # Create a custom Key for elasticsearch index.
                ikey = d_doc['flight'] + '-' + d_doc['aircraft'] + '-' + d_doc['datetime'][:10]
                # Get the response of the insert.
                res = es.index(index="air_traffic_index", doc_type='maps', id=ikey, body=d_doc)
                print('ElasticSearch Response: ', res['result'])
        except KeyboardInterrupt:
            pass

  
    # Connecting to Satori live data
    with make_client(endpoint=endpoint, appkey=appkey) as client:
        print('Connected to Satori RTM!')

        # Subscription class.
        class SubscriptionObserver(object):
            def __init__(self, channel):
                self.message_count = 0
                self.channel = channel

            # Function triggered when the messages are coming.    
            def on_subscription_data(self, data):
                # Loop to get all the messages.
                for message in data['messages']:
                    # Filter according to parameter in the command line.
                    if message['aircraft'] == sfilter or sfilter == 'ALL':
                        if message['flight'] == sflight or sflight == 'ALL':
                            # It goes on only if the flight is not empty.
                            if message['flight'] != '':
                                # Store the message in a dictionary variable type.
                                d_msg = message                            
                                # Enrichment of dictionary variable with important information to become easier futures queries.
                                # Datatime in the readable format, location and airline name
                                d_msg['datetime'] = datetime.datetime.fromtimestamp(int(message["time"])).strftime('%Y-%m-%dT%H:%M:%S')
                                d_msg['location'] = str(message['lat']) + ',' + str(message['lon'])
                                d_msg['code']     = str(message['flight'][:2])
                                p_tmp             = p_airlines.query('code == "' + d_msg['code'] + '"')
                                d_msg['airline']  = p_tmp.to_string(index=False, header=False)

                                # Print message to see the process flowing.
                                print("message :", d_msg)

                                # Send data to kafka - producer.
                                producer.send('airtraffic', d_msg)

                                # Calling the function that persists data on elasticsearch.
                                insert_data_es(d_msg)

            # Function triggered when enter on subscribe.
            def on_enter_subscribed(self):
                print('Subscription is now active')
                print(sfilter)
                print(sflight)

            def on_deleted(self):
                print('Received {0} messages from channel ""{1}""'.format(
                            self.message_count, self.channel))

        # Subscribe on the satori webserver.
        subscription_observer = SubscriptionObserver(channel)
        client.subscribe(
                channel,
                SubscriptionMode.ADVANCED,
                subscription_observer)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
