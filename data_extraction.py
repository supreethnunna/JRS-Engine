from __future__ import print_function # Python 2/3 compatibility

import boto3
import json
import decimal
########################
import difflib
import sys
import pymongo
import numpy as np
import memcache
import hashlib
######################
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError











dynamodb = boto3.resource('dynamodb', aws_access_key_id="anything", aws_secret_access_key="anything", region_name="us-west-2",endpoint_url="http://localhost:8000")


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


table = dynamodb.Table('jobdesc')
response = table.query(
KeyConditionExpression=Key('jobid').eq('j1')
            )
for item in response['Items']:
    a = item['skills']
    #b1 = a['competency']
    b = item['Requirements']
    c = item['PositionTitle']
    d = item['competencies']

#print(d)


for val in d:
    print(val)
#print(a)
#print(d)


#KeyConditionExpression=Key('jobid').eq('j1')
#result = table.scan('jobdesc',
 ##                       {'competencies': [{'S': ''}],
   #                               'ComparisonOperator': 'CONTAINS'})
#

#res = item.where(:d => "Marketing Management").count


#response = table.scan(
    #ConditionExpression=Key('jobid').eq('j1')
    #ConditionExpression = "Attr('comp')contains(skills, :value)",
    #FilterExpression = "contains(comp, :val)",
    #ConditionExpression = "attribute_exists(comp)",
    
    
    #ExpressionAttributeValues={'val': 'Marketing Management'}
#)



#response1 = table.scan(
#                  Select= 'ALL_ATTRIBUTES',
#                  ExpressionAttributeNames = {"jobid": 'j1'},
#                  #FilterExpression: "PostedBy = :val"
#                  FilterExpression=Attr('competencies').contains("Mba")
                  
#                  )



#print(response)

array = []
for i in a:
    z = i['competency']
    array.append(z)
#print(array)



