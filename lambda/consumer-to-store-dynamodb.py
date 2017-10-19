from __future__ import print_function

import base64
import json
import boto3
import random
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.getenv('TABLE_NAME'))

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        if data.has_key('time'):
            table.put_item(Item=data)
        else:
            print("The record has no time field: " + json.dumps(data))

    return 'Successfully processed {} records.'.format(len(event['Records']))
