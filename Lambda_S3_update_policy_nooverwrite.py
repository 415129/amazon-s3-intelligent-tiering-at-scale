#amazon-s3-intelligent-tiering-at-scale
import json
import os
import boto3
import logging
from botocore.exceptions import ClientError
#from tqdm import tqdm

logging.basicConfig(level=logging.INFO,format='%(levelname)s: %(asctime)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#s3_client = boto3.resource('s3')
s3_resource = boto3.resource('s3')
client = boto3.client('sts')
bucket_tag_key = "storage.class"
bucket_tag_value = "s3.it"
TransitionStatus = []

def lambda_handler(event,context):
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    lifecycle_config_settings_it = {
    'Rules': [
        {'ID': 'S3 Intelligent Tier Transition Rule',
         'Filter': {'Prefix': ''},
         'Status': 'Enabled',
         'Transitions': [
             {'Days': 0,
              'StorageClass': 'INTELLIGENT_TIERING'}
         ]}
    ]}

    archive_policy = {
        'Id': 'Archive_Tier', 
        'Status': 'Enabled',
        'Tierings': [
            {
                'Days': 90,
                'AccessTier': 'ARCHIVE_ACCESS'
            }, 
            {
                'Days': 180,
                'AccessTier': 'DEEP_ARCHIVE_ACCESS'
            }
        ]
    }
    ID = ('Archive_Tier')
    #ds-demo-bucket
    bucket_tag_key = "storage.class"
    bucket_tag_value = "s3.it"
    print(s3_client.list_buckets())
    print(bucket_tag_value)
    BucketName = s3_client.list_buckets()
    #bucketName=event['detail']['requestParameters']['bucketName']
    for bucket in BucketName['Buckets']:
        try:
            bucketName = bucket['Name']
            logging.info(f'bucket name = {bucket['Name']}')
            tag_set = s3_resource.BucketTagging(bucket['Name']).tag_set
            for tag in tag_set:
                tag_values = list(tag.values())
                logging.info(f'tag key ={tag["Key"]}' )
                logging.info(f'tag value ={tag["Value"]}' )
                if (tag['Key'] == bucket_tag_key):
                    if(tag['Value'] == bucket_tag_value):
                        logging.info (f'TAG Match: tag key={bucket_tag_key} and tag value={bucket_tag_value} for bucket {bucketName}' )
                        put_bucket_lifecycle_configuration(bucketName,lifecycle_config_settings_it )
                        modify_bucket_objects(bucketName)
                        put_bucket_intelligent_tiering_configuration(bucketName, archive_policy, ID)
        except ClientError as e:
            logging.info(f'No Tags')

    logger.info(f'S3 Bucket created  event handled OK')

def modify_bucket_objects(Name):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            #print(my_bucket_object.key)
            object = s3_resource.Object(Name,my_bucket_object.key)
            #print(object)
            logging.info(f'Bucket = {Name}:Changing Storage Class for {my_bucket_object.key}')
            object.put(StorageClass='INTELLIGENT_TIERING')
    except ClientError as err:
        print (err.response['Error']['Code'])  
    
    
def getAccountID():
    account_id = client.get_caller_identity()
    return(account_id['Account'])
    #account_id = iam.CurrentUser().arn.split(':')[4]
    #return(account_id)

def modify_bucket_objects(Name):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            #print(my_bucket_object.key)
            object = s3_resource.Object(Name,my_bucket_object.key)
            print(object)
            object.put(StorageClass='INTELLIGENT_TIERING')
    except ClientError as err:
        print (err.response['Error']['Code'])  
        
def put_bucket_lifecycle_configuration(Name, lifecycle_config):
    ownerAccountId = getAccountID()
    try:
        result = s3_client.get_bucket_lifecycle_configuration(Bucket=Name, ExpectedBucketOwner=ownerAccountId)
        Rules= result['Rules']
        # Scenario #1 
        if any("Transitions" in keys for keys in Rules):
            for Rule in Rules:
                for key, value in Rule.items():
                    if (key == 'Transitions'):
                        Days = value[0]['Days']
                        StorageClass = value[0]['StorageClass']
                        TransitionStatus.append(Name)
                        TransitionStatus.append(Days)
                        TransitionStatus.append(StorageClass)
                        TransitionStatus.append('No changes made to S3 Lifecycle configuration')
        else:
            # Scenario #2
            policy = lifecycle_config 
            for p in policy['Rules']:
                for key, value in p.items():
                    if key =='Transitions':
                        Days = value[0]['Days']
                        StorageClass = value[0]['StorageClass']
                        TransitionStatus.append(Name)
                        TransitionStatus.append(Days)
                        TransitionStatus.append(StorageClass)
                        TransitionStatus.append('Updated the existing Lifecycle with Transition rule to S3 INT')

            Rules.append(policy['Rules'][0])
            lcp = s3_client.put_bucket_lifecycle_configuration(
                Bucket=Name, LifecycleConfiguration = {'Rules':Rules })
    except ClientError as err:
        # Scenario #3
        # Catching a situation where bucket does not belong to an account
        print (err.response['Error']['Code'])
        if err.response['Error']['Code'] == 'AccessDenied':
            print ("This account does not own the bucket {}.".format(Name))
            Days = 'N/A'
            StorageClass ='N/A'
            TransitionStatus.append(Name)
            TransitionStatus.append(Days)
            TransitionStatus.append(StorageClass)
            TransitionStatus.append("The Bucket "+Name+" does not belong to the account-"+ownerAccountId)
        elif err.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            print("This bucket {} has no LifeCycle Configuration".format(Name)) 
            policy = lifecycle_config 
            for p in policy['Rules']:
                for key, value in p.items():
                    if key =='Transitions':
                        Days = value[0]['Days']
                        StorageClass = value[0]['StorageClass']
                        TransitionStatus.append(Name)
                        TransitionStatus.append(Days)
                        TransitionStatus.append(StorageClass)
                        TransitionStatus.append('Added a new S3 Lifecycle Transition Rule to S3 INT')
            lcp = s3_client.put_bucket_lifecycle_configuration(
                    Bucket=Name, LifecycleConfiguration = policy)
        else:
            print ("err.response['Error']['Code']")


def put_bucket_intelligent_tiering_configuration(bucket_name, archive_policy, id):
    """Set the archive tier policy for an Amazon S3 bucket
    :param bucket_name: string
    :param intel_config: dict of intelligence tier configuration settings
    :param Id = Archive ID
    :return: True if archive policy configuration was set, otherwise False
    """
    try:
        ok = s3_client.put_bucket_intelligent_tiering_configuration(Bucket=bucket_name,
                                              IntelligentTieringConfiguration=archive_policy, Id=id)
        if ok:
            logging.info(f'The archive configuration was set for {bucket_name}')
        else:
            logger.error(f'Could not set archive configuration  for {bucket_name}')
            
    except ClientError as e:
        logging.error(e)
        return False
    return True