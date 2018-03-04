from __future__ import print_function
from pprint import pprint
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
import urllib
import os

esIndex = os.environ['esIndex']
esEndPoint = os.environ['esEndPoint']

print('Loading function')

def connectES(esEndPoint):
    print ('Connecting to the ES Endpoint {0}'.format(esEndPoint))
    try:
        esClient = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)
        return esClient
    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)
        exit(3)

def addDocElement(esClient, record, esIndex):
    try:
        ddb_record = json.dumps(dict(record))
        print('TESTING:' + ddb_record)
        esClient.index(index=esIndex, doc_type='dynamoRecord', body=ddb_record)
        return 1
    except Exception as E:
        print("Error: ",E)
        exit(5)

def lambda_handler(event, context):
    esClient = connectES(esEndPoint)
    print ('Client created')
    for record in event['Records']:
        try:
            addDocElement(esClient, record, esIndex)
        except Exception as e:
            print(e)
            print('Error adding object to ElasticSearch Domain.')
            raise e