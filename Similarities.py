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
#dynamodb = boto3.resource('dynamodb', aws_access_key_id="anything", aws_secret_access_key="anything", region_name="us-west-2",endpoint_url="http://localhost:8000")
import boto.dynamodb
dynamodb = boto.dynamodb.connect_to_region( 'us-east-1')

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
        #print(a)
        job_degrees = []
        for deg in b:
            job_degrees.append(str(deg.get('Degree')))
        comp = []
        comp_id = []
        tax_id = []
        for com in a:
            comp.append(str(com.get('competency')))
            comp_id.append(str(com.get('comp_id')))
            tax_id.append(str(com.get('tax_id')))
            
        PosTitle = []
        #for pos in c:
            #PosTitle.append(str(pos.get('Title')))
        PosTitle.append(c)
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
        #print(comp_id)
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
    elif degree == "high school diploma":
        return(3)
    elif degree == "certificate":
        return(0)
    else:
        return(0)

def deg_sim(r_deg_val,j_deg_min_val):
    #print(r_deg_val)
    #print(j_deg_min_val)
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
                
    #print(a,b)
    #a [comp_id,tax_id]resume
    #b [comp_id,tax_id]jobdesc
    #print(a)
    #print(b)
    #print("SSSSSSSSSSS")
    a[0] = str(a[0])
    #print(a[1])
    b[0] = str(b[0])
    #print(b[1])
    #print("compdid_a")
    #print(a[0])
    #print("compdid_b")
    #print(b[0])
    #print("taxid_a")
    #print(a[1])
    #print("taxid_b")
    #print(b[1])
    if b[1].strip() != "" and b[1] != None:
        bsplit = b[1].split()
        #print(bsplit)
    else:
        bsplit = []
        bsplit.append("none")
    #print("Aone:"+a[1])
    if a[1].strip() != "" and a[1] != None:
        asplit = a[1].split()
        #print(asplit)
    else:
        asplit = []
        asplit.append("none1")

    #print(asplit)
    #skill A is hypernym of B
    if a[0].strip() == b[0].strip():
        #print("True")
        return True
        
    if a[0].strip() in bsplit[0].strip():
        #print("True")
        return True
        
    #skill B is hypernym of A
    elif b[0].strip() in asplit[0].strip():
        #print("True")
        return True
        
    #skill A and B are homonyms
    #elif any(x in asplit[0] for x in bsplit[0]):
    elif asplit[0].strip() == bsplit[0].strip():
        return True
    else:
        #print("False")
        return False
            

################################################################################################
def Jaccard_jobseeker(server_name,rid,skill_r,skill_j):
    table = server_name.Table('resume')
    response = table.query(
    KeyConditionExpression=Key('rid').eq(rid)
            )
    for item in response['Items']:
        competencies = item['competencies']
        
    skill1 = False
    skill2 = False
    
    if skill_r in competencies:
        skill1 = True
    if skill_j in competencies:
        skill2 = True
    both_skills = 0
    atleastOne_skill = 0
    if skill1 == True and skill2== True:
        both_skills = 1
    if skill1 == True or skill2== True:
        atleastOne_skill = 1

    table = server_name.Table('jobdesc')

    response1 = table.scan(
                Select= 'ALL_ATTRIBUTES',
                FilterExpression= Attr("competencies").contains(skill_r) | Attr("competencies").contains(skill_j)
                )

    response2 = table.scan(
                Select= 'ALL_ATTRIBUTES',
                FilterExpression= Attr("competencies").contains(skill_r) & Attr("competencies").contains(skill_j)
                )
    return atleastOne_skill + response1["Count"], both_skills + response2["Count"]
    #return atleastOne_skill,both_skills,response1["Count"],response2["Count"]

def Jaccard_employer(server_name,jid,skill_r,skill_j):
    table = server_name.Table('jobdesc')
    response = table.query(
    KeyConditionExpression=Key('jobid').eq(jid)
            )
    for item in response['Items']:
        competencies = item['competencies']
        
    skill1 = False
    skill2 = False
    
    if skill_r in competencies:
        skill1 = True
    if skill_j in competencies:
        skill2 = True
    both_skills = 0
    atleastOne_skill = 0
    if skill1 == True and skill2== True:
        both_skills = 1
    if skill1 == True or skill2== True:
        atleastOne_skill = 1

    table = server_name.Table('resume')

    response1 = table.scan(
                Select= 'ALL_ATTRIBUTES',
                FilterExpression= Attr("competencies").contains(skill_r) | Attr("competencies").contains(skill_j)
                )

    response2 = table.scan(
                Select= 'ALL_ATTRIBUTES',
                FilterExpression= Attr("competencies").contains(skill_r) & Attr("competencies").contains(skill_j)
                )
    return atleastOne_skill + response1["Count"],both_skills + response2["Count"]
    #return atleastOne_skill,both_skills,response1["Count"],response2["Count"]


   

################################################################################################
def sim_all_features_resume(server_name,jobid,rid):

    jobpost_features = get_features(server_name, "jobdesc",jobid)
    j_competency = jobpost_features[0]
    j_comp_id = jobpost_features[1]
    #print(j_comp_id)
    j_tax_id = jobpost_features[2]
    #print(j_tax_id)
    j_PosTitle = jobpost_features[3]
    j_degrees = jobpost_features[4]
    #print(j_degrees)
    #print(j_PosTitle)
    resume_features = get_features(server_name, "resume",rid)
    r_competency = resume_features[0]
    r_comp_id = resume_features[1]
    r_tax_id = resume_features[2]
    r_Title = resume_features[3]
    r_degrees = resume_features[4]
    #print(r_degrees)
    #print(r_Title)
    
    #Degree Similarity
    r_degree_vals =[]
    for i in r_degrees:
        r_degree_vals.append(deg_level(i))
    #print(r_degree_vals)
    j_degree_vals =[]
    for i in j_degrees:
        j_degree_vals.append(deg_level(i))
    #print(j_degrees)      
    if (j_degree_vals != None):
        degree_sim = deg_sim(max(r_degree_vals),max(j_degree_vals))
    else:
        degree_sim = 1
    #print(degree_sim)
    #Title sim
    titleSim_arr = [] 
    if (r_Title != None):
        if (j_PosTitle != None):
            for title in r_Title:
                titleSim_arr.append(tit_sim(j_PosTitle[0],title))
            titleSim = max(titleSim_arr)
        else:
            titleSim = 0
    else:
        titleSim = 0
    #return(degree_sim, titleSim)
    #print(titleSim)
    #SkillSim
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)

    competencies_r = r_competency
    #print(competencies_r)
    comp_id_r = r_comp_id
    #print(comp_id_r)
    #print(comp_id_r[0])
    tax_id_r = r_tax_id
    #print(tax_id_r)
    #print(tax_id_r[0])
    competencies_j = j_competency
    #print(competencies_j)
    comp_id_j = j_comp_id
    #print(comp_id_j)
    #print(comp_id_j[0])
    tax_id_j = j_tax_id
    #print(tax_id_j)
    #print(tax_id_j[0])
    simarray=[]
    for i in range(len(competencies_j)):
        sim = []
        for j in range(len(competencies_r)):
            #print(skillSimCheck([comp_id_r[j],tax_id_r[j]],[comp_id_j[i],tax_id_j[i]]))
            if skillSimCheck([comp_id_r[j],tax_id_r[j]],[comp_id_j[i],tax_id_j[i]]):
                a1 = str(competencies_r[j])
                #print a1
                b1 = str(competencies_j[i])
                key1 = hashlib.sha256(a1).hexdigest()
                key2 = hashlib.sha256(b1).hexdigest()
                c1 = key1 + key2
                #print(c1)
                d1 = key2 + key1
                if (mc.get(c1) == None and  mc.get(d1) == None):
                    a,b = Jaccard_jobseeker(server_name,rid,competencies_r[j].strip(),competencies_j[i].strip())
                    #print(a,b)
                    val = str((b)/float(a))
                    #print(val)
                    mc.set(c1,val)
                    sim.append(val)
                elif (mc.get(c1) == None and mc.get(d1) != None):
                    saved_val = mc.get(d1)
                    #print(saved_val)
                    sim.append(saved_val)
                else:
                    saved_val = mc.get(c1)
                    sim.append(saved_val)
                    #print(saved_val)
            else:
                sim.append('0')
        simarray.append(max(sim))
    numarr =[]
    #print(simarray)
    for i in simarray:
        numarr.append(float(i))
    #print(degree_sim,titleSim,np.mean(numarr))
    return degree_sim,titleSim,np.mean(numarr)

#print(sim_all_features_resume(dynamodb,"j7","r1"))

#a = sim_all_features_resume(dynamodb,'j1','r2')
#print(a)
####################################################################################################################    
def sim_all_features_employer(server_name,jobid,rid):

    jobpost_features = get_features(server_name, "jobdesc",jobid)
    j_competency = jobpost_features[0]
    j_comp_id = jobpost_features[1]
    j_tax_id = jobpost_features[2]
    j_PosTitle = jobpost_features[3]
    j_degrees = jobpost_features[4]
    #print(j_degrees)
    #print(j_PosTitle)
    resume_features = get_features(server_name, "resume",rid)
    r_competency = resume_features[0]
    r_comp_id = resume_features[1]
    r_tax_id = resume_features[2]
    r_Title = resume_features[3]
    r_degrees = resume_features[4]
    #print(r_degrees)
    #print(r_Title)
    
    #Degree Similarity
    r_degree_vals =[]
    for i in r_degrees:
        r_degree_vals.append(deg_level(i))
    #print(r_degree_vals)
    j_degree_vals =[]
    for i in j_degrees:
        j_degree_vals.append(deg_level(i))
    #print(j_degrees)      
    if (j_degree_vals != None):
        degree_sim = deg_sim(max(r_degree_vals),max(j_degree_vals))
    else:
        degree_sim = 1
    #print(degree_sim)
    #Title sim
    titleSim_arr = [] 
    if (r_Title != None):
        if (j_PosTitle != None):
            for title in r_Title:
                titleSim_arr.append(tit_sim(j_PosTitle[0],title))
            titleSim = max(titleSim_arr)
        else:
            titleSim = 0
    else:
        titleSim = 0
    #return(degree_sim, titleSim)
    #print(titleSim)
    #SkillSim
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)

    competencies_r = r_competency
    comp_id_r = r_comp_id
    #print(comp_id_r)
    #print(comp_id_r[0])
    tax_id_r = r_tax_id
    #print(tax_id_r)
    #print(tax_id_r[0])
    competencies_j = j_competency
    
    comp_id_j = j_comp_id
    #print(comp_id_j)
    #print(comp_id_j[0])
    tax_id_j = j_tax_id
    #print(tax_id_j)
    #print(tax_id_j[0])
    simarray=[]
    for i in range(len(competencies_j)):
        sim = []
        for j in range(len(competencies_r)):
            if skillSimCheck([comp_id_r[j],tax_id_r[j]],[comp_id_j[i],tax_id_j[i]]):
                a1 = str(competencies_r[j])
                #print a1
                b1 = str(competencies_j[i])
                key1 = hashlib.sha256(a1).hexdigest()
                key2 = hashlib.sha256(b1).hexdigest()
                c1 = key1 + key2
                #print c1
                d1 = key2 + key1
                if (mc.get(c1) == None and  mc.get(d1) == None):
                    a,b = Jaccard_employer(server_name,jobid,competencies_r[j].strip(),competencies_j[i].strip())
                    val = str((b)/float(a))
                    mc.set(c1,val)
                    sim.append(val)
                elif (mc.get(c1) == None and mc.get(d1) != None):
                    saved_val = mc.get(d1)
                    sim.append(saved_val)
                else:
                    saved_val = mc.get(c1)
                    sim.append(saved_val)
            else:
                sim.append('0')
        simarray.append(max(sim))
    numarr =[]
    #print(simarray)
    for i in simarray:
        numarr.append(float(i))
    #print(degree_sim,titleSim,np.mean(numarr))
    return degree_sim,titleSim,np.mean(numarr)    

#print(sim_all_features_employer(dynamodb,"j60","r5"))


#a,b = Jaccard_jobseeker(dynamodb,"r1","Database","Database Applications")
#print(a,b)
