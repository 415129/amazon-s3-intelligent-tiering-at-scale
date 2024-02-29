import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,format='%(levelname)s: %(asctime)s: %(message)s')
# boto3_session = boto3.session.Session(profile_name='967655172285_ie_dev_AdministratorAccess')
boto3_session = boto3.session.Session()
s3_client = boto3_session.resource('s3')
s3_resource=boto3_session.resource('s3')

def visit_buckets() :
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
    bucket_tag_key = "storage.class"
    bucket_tag_value = "s3.it"
    for bucket in s3_client.buckets.all():
        try:
            logging.info(f'bucket name = {bucket.name}')
            tag_set = s3_client.BucketTagging(bucket.name).tag_set
            for tag in tag_set:
                tag_values = list(tag.values())
                logging.info(f'tag key ={tag["Key"]}' )
                logging.info(f'tag value ={tag["Value"]}' )
                if (tag['Key'] == bucket_tag_key):
                    if(tag['Value'] == bucket_tag_value):
                      logging.info (f'TAG Match: tag key={bucket_tag_key} and tag value={bucket_tag_value} for bucket {bucket.name}' )
                      put_bucket_lifecycle_configuration(bucket.name,lifecycle_config_settings_it )  
                    #   put_bucket_intelligent_tiering_configuration(bucket.name, archive_policy, ID)
        except ClientError as e:
            logging.info(f'No Tags')

def put_bucket_lifecycle_configuration(bucket_name, lifecycle_config):
    """Set the lifecycle configuration of an Amazon S3 bucket
    :param bucket_name: string
    :param lifecycle_config: dict of lifecycle configuration settings
    :return: True if lifecycle configuration was set, otherwise False
    """
    try:
        ok = boto3_session.client('s3').put_bucket_lifecycle_configuration(Bucket=bucket_name,
                                              LifecycleConfiguration=lifecycle_config)
        if ok:
            logging.info(f'The lifecycle configuration was set for {bucket_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True

def put_bucket_intelligent_tiering_configuration(bucket_name, archive_policy, id):
    """Set the archive tier policy for an Amazon S3 bucket
    :param bucket_name: string
    :param intel_config: dict of intelligence tier configuration settings
    :param Id = Archive ID
    :return: True if archive policy configuration was set, otherwise False
    """
    try:
        ok = boto3_session.client('s3').put_bucket_intelligent_tiering_configuration(Bucket=bucket_name,
                                              IntelligentTieringConfiguration=archive_policy, Id=id)
        if ok:
            logging.info(f'The archive configuration was set for {bucket_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True

def modify_bucket_objects(Name):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            #print(my_bucket_object)
            object = s3_resource.Object(Name,my_bucket_object.key)
            #print(object.storage_class)
            #if object.storage_class != 'INTELLIGENT_TIERING' :
            if object.storage_class is None :
                #print(object.key,object.storage_class)
                logging.info(f'Bucket = {Name}:Changing Storage Class for {my_bucket_object.key} from {object.storage_class} or STANDARD')
                object.put(StorageClass='INTELLIGENT_TIERING')
                # accepted values are 'STANDARD' |'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'
    except ClientError as err:
        print (err.response['Error']['Code'])  

if __name__ == '__main__' :
    logging.info(f'{__file__}')
    #visit_buckets()
    modify_bucket_objects('ds-demo-bucket')









