import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
from time import strftime 
from tqdm import tqdm
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import Font
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string, get_column_letter


s3 = boto3.client('s3')
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
iam = boto3.resource('iam')
client = boto3.client('sts')
ignorelist=[]
servicelist=['vpc','s3','elb','CloudTrail','rds','']
region=['us-east-1','us-west-1','ca-central-1','eu-west-2','us-west-2','us-east-2','ap-south-1']
column_list=[None, 'ID', 'Filter', 'Status', 'AbortIncompleteMultipartUpload', 'Prefix', 'Expiration', 'Transitions', 'NoncurrentVersionExpiration', 'NoncurrentVersionTransitions']
stdpname = ['MMS_Versioning_Policy_3v_retains','MMSDeletionStandardPolicy','MMSDeleteMarkers','AbortIncompleteMultipartUploadsRule','MMSVersioningPolicy']
## Global list variable to keep track of the Bucket Name, Transition Days, StorageClass, Status  
TransitionStatus = []

policy = {}
ruledict= {}


MMSMoveCustomPolicy = {
    'Rules': [
        {
        'ID': 'MMSMoveCustomPolicy',
        'Expiration': {'Days': 2555},
        'Filter': {'ObjectSizeGreaterThan': 131072}, 
        'Status': 'Enabled',
        'Transitions': [{'Days': 121, 'StorageClass': 'GLACIER_IR'}]
        }
        ]
    }

def put_bucket_lifecycle_configuration_standard(Name, lifecycle_config):
    ownerAccountId = getAccountID()
    try:
        result = s3.get_bucket_lifecycle_configuration(Bucket=Name, ExpectedBucketOwner=ownerAccountId)
        #print(result)
        Rules= result['Rules']
        if lifecycle_config['Rules'][0]['ID'] == 'MMSDeleteMarkers':            
            for target in Rules:
                try:
                    #print(target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'])               
                    if target['Expiration']['ExpiredObjectDeleteMarker'] == 'Ture' or target['ID'] not in stdpname:
                        print('0-Deleteing LCP from Bucket = ' + Name + ' ,LCP = ' + target['ID'])
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                except  KeyError:
                    continue
        elif lifecycle_config['Rules'][0]['ID'] == 'AbortIncompleteMultipartUploadsRule':
            for target in Rules:
                try:
                    #print(target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'])               
                    if target['AbortIncompleteMultipartUpload']['DaysAfterInitiation'] > 6 or target['ID'] not in stdpname :
                        print('1-Deleteing LCP from Bucket = ' + Name + ' ,LCP = ' + target['ID'])
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                    
                        
                except  ClientError as err:
                    print(err)
                    if err.response['Error']['Code'] == 'InvalidRequest':
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                except  KeyError:
                    continue
        elif lifecycle_config['Rules'][0]['ID'] == 'MMSVersioningPolicy':
            for target in Rules:
                try:
                    
                    if (target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'] > 1 and target['NoncurrentVersionExpiration']['NoncurrentDays'] > 31) or target['Expiration']['Days'] > 31 or target['ID'] not in stdpname:
                        #print(target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'] ,target['NoncurrentVersionExpiration']['NoncurrentDays'] , target['Expiration']['Days'] > 31 or target['ID'])               
                        print('2-Deleteing LCP from Bucket = ' + Name + ' ,LCP = ' + target['ID'])
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                except  KeyError:
                    continue
        elif lifecycle_config['Rules'][0]['ID'] == 'MMS_Versioning_Policy_3v_retains':
            for target in Rules:
                try:                    
                    if (target['NoncurrentVersionExpiration']['NoncurrentDaystarget'] == 1 and target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'] > 3) or target['ID'] not in stdpname :
                        #print(target['NoncurrentVersionExpiration']['NewerNoncurrentVersions'] ,target['NoncurrentVersionExpiration']['NoncurrentDays'] , target['Expiration']['Days'] > 31 or target['ID'])               
                        print('21-Deleteing LCP from Bucket = ' + Name + ' ,LCP = ' + target['ID'])
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                    else:
                        Rules.append(lifecycle_config['Rules'][0])
                except  KeyError:
                    continue                
     
        
        
        lpolicy = lifecycle_config
        Rules.append(lpolicy['Rules'][0])
        #print(Rules)
        lcp = s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
        #print(lcp)
    except ClientError as err:
        if err.response['Error']['Code'] == 'AccessDenied':
            print ("This account does not own the bucket {}.".format(Name))
    except ClientError as err:
        print(err.response)
        if err.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules': lifecycle_config['Rules'] })            
        elif  err.response['Error']['Code'] == 'InvalidRequest':
            print(Rules,err.response)
        elif err.response['Error']['ArgumentName'] == 'ID':
            print( err.response['Error']['ArgumentValue'] + ' already exists and skipping rule')

            


def put_bucket_lifecycle_configuration_custom(Name, lifecycle_config,pname):
    ownerAccountId = getAccountID()
    try:
        result = s3.get_bucket_lifecycle_configuration(Bucket=Name, ExpectedBucketOwner=ownerAccountId)
        #print(result)
        Rules= result['Rules']
        for r1 in Rules:            
            if lifecycle_config[r1][0]['ID'] == pname:            
                for target in Rules:
                    try:
                        Rules.remove(target)
                        s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
                    except  KeyError:
                        continue
        
        policy = lifecycle_config
        Rules.append(policy['Rules'][0])
        #print(Rules)
        lcp = s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
    except ClientError as err:
        #print(err.response)
        if err.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            s3.put_bucket_lifecycle_configuration(Bucket=Name, LifecycleConfiguration = {'Rules': lifecycle_config['Rules'] })            
        elif err.response['Error']['ArgumentName'] == 'ID':
            print( err.response['Error']['ArgumentValue'] + ' already exsists and skipping rule')
        elif err.response['Error']['Code'] == 'InvalidRequest':
            print(err.response)




# method to determine accountID 
def getAccountID():
    account_id = client.get_caller_identity()
    return(account_id['Account'])
    #account_id = iam.CurrentUser().arn.split(':')[4]
    #return(account_id)



    

def updatelcp(filename):
    wb = load_workbook(filename)
    ws = wb.active

    max_row=ws.max_row
    max_col=ws.max_column

    #print(max_row,max_col)
            
            
                       
    for row_cells in ws.iter_rows(min_row=2, max_row=max_row):
            counter = 0
            for i, cell in enumerate(row_cells):

                counter += 1
                #print(get_column_letter(counter) + str(1))
                colname = ws[get_column_letter(counter) + str(1)]
                #print(colname.value,cell.value)
                if colname.value is None:
                    bucket_name = cell.value
                    print(bucket_name)                    
                elif colname.value == 'ID':
                    ID = cell.value
                    print(ID)
                elif colname.value == 'Expiration':
                    Expiration = cell.value
                elif colname.value == 'Filter':
                    Filter = cell.value
                elif colname.value == 'Status':
                    Status = cell.value
                elif colname.value == 'Transitions':
                    Transitions = cell.value
                    print(Transitions) 
            lifecycle_config = f"""{{
                'Rules': [
                    {{
                    'ID': '{ID}',
                    'Expiration': {Expiration},
                    'Filter': {Filter}, 
                    'Status': '{Status}',
                    'Transitions': {Transitions}
                    }}
                    ]
                }}"""
            
            print(lifecycle_config)
    
            #put_bucket_lifecycle_configuration_custom(bucket_name, lifecycle_config,ID)

                                                          
                
        
        

    
def main():
    updatelcp('postchange813408048622.xlsx')
    
    
    
if __name__ == "__main__":
    main()