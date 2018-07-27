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
dynamodb = boto3.resource('dynamodb', aws_access_key_id="******", aws_secret_access_key="****", region_name="****",endpoint_url="****")


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
###############################################################################################
def get_features(server_name,doc,doc_id):
    if doc == "jobdesc":
        table = server_name.Table('jobdesc')
        response = table.query(
        KeyConditionExpression=Key('jobid').eq(doc_id)
        )
        for item in response['Items']:
            a = item['skills']
            b = item['Requirements']
            c = item['PositionTitle']
            d = item['compentencies']
        #print(a)
        job_degrees = []
        for deg in b:
            job_degrees.append(str(deg.get('Degree')))
        comp = []
        comp_id = []
        tax_id = []
        for com in a:
            comp.append(str(com.get('comp_id')))
            comp_id.append(str(com.get('competency')))
            tax_id.append(str(com.get('tax_id')))
            
        PosTitle = []
        for pos in c:
            PosTitle.append(str(pos.get('Title')))
        return(comp,comp_id,tax_id,PosTitle,job_degrees)
    elif (doc == "resume"):
        table = server_name.Table('resume')
        response = table.query(
        KeyConditionExpression=Key('rid').eq(doc_id)
        )
        for item in response['Items']:
            a = item['skills']
            b = item['education']
            c = item['EmploymentHistory']

        r_degrees = []
        for deg in b:
            r_degrees.append(str(deg.get('degreetype')))
        comp = []
        comp_id = []
        tax_id = []
        for com in a:
            comp.append(str(com.get('competency')))
            comp_id.append(str(com.get('comp_id')))
            tax_id.append(str(com.get('tax_id')))

        PosTitle = []
        for pos in c:
            PosTitle.append(str(pos.get('Title')))
        return(comp,comp_id,tax_id,PosTitle,r_degrees)  
    else:
        return "0"


#wfw = get_features(dynamodb,"jobdesc","j1")
#print(wfw[comp])
#print(get_features(dynamodb,"resume","r1"))
#########################################################################################
def deg_level(degree):
    if degree == "bachelors":
        return(1)
    elif degree == "masters":
        return(2)
    elif degree == "doctoral":
        return(3)
    elif degree == "":
        return(0)

def deg_sim(r_deg_val,j_deg_min_val):
    if j_deg_min_val != 0:
        if (r_deg_val - j_deg_min_val)== 2 :
            return(0.5)
        elif (r_deg_val - j_deg_min_val)== 1 :
            return(1)
        elif (r_deg_val - j_deg_min_val)== 0 :
            return(1)
        elif (r_deg_val - j_deg_min_val) < 0 :
            return(0)
    else:
        return(0.5)
        
def maj_sim(maj_r,maj_j):
    if maj_r == maj_j:
        return(1)
    else:
        return(0)

def tit_sim(a,b):
    seq = difflib.SequenceMatcher(a = a.lower(),b = b.lower())
    return(seq.ratio())

def skillSimCheck(a,b):
        #a [comp_id,tax_id]resume
        #b [comp_id,tax_id]jobdesc
        
        a[0] = str(a[0])
        b[0] = str(b[0])
        
        if b[1] != "":
            bsplit = b[1].split()
        else:
            bsplit = []
            bsplit.append("none")

        if a[1] != "":
            asplit = a[1].split()
        else:
            asplit = []
            asplit.append("none1")
            
        #skill A is hypernym of B
        if a[0] == b[0]:
            return True
        if a[0] in bsplit[0]:
            return True
        #skill B is hypernym of A
        elif b[0] in asplit[0]:
            return True
        #skill A and B are homonyms
        #elif any(x in asplit[0] for x in bsplit[0]):
        elif asplit[0] == bsplit[0]:
            return True
        else:
            return False

def Jaccard_jobseeker_jcount(server_name,skill_r,skill_j):
        table = server_name.Table('jobdesc')
        response = table.query(
        KeyConditionExpression=Key('jobid').eq('j1')
        FilterExpression = "(competencies = :value1) AND (competencies = :value2)",
        ExpressionAttributeValues={
        ':value1' : "Accounting",
        ':value2' : "Mba"}
        }
        
        abc =[]
        for item in response['Items']:
            abc = item['skills']
        print(abc)


Jaccard_jobseeker_jcount(dynamodb,3,4)
