import boto3
import logging
from botocore.exceptions import ClientError
from configparser import ConfigParser
from datetime import datetime, timedelta
import re

file = "config.ini"    # give the path to the file
# config = ConfigParser()
# config.read(file)

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s: %(asctime)s: %(message)s')
# boto3_session = boto3.session.Session(profile_name='967655172285_ie_dev_AdministratorAccess')
boto3_session = boto3.session.Session()
s3_client = boto3_session.resource('s3')
s3_resource = boto3_session.resource('s3')


def visit_buckets():
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
                logging.info(f'tag key ={tag["Key"]}')
                logging.info(f'tag value ={tag["Value"]}')
                if (tag['Key'] == bucket_tag_key):
                    if (tag['Value'] == bucket_tag_value):
                        logging.info(f'TAG Match: tag key={bucket_tag_key} and tag value={bucket_tag_value} for bucket {bucket.name}')
                        put_bucket_lifecycle_configuration(
                            bucket.name, lifecycle_config_settings_it)
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


def set_object_tags(bucket, object, update=True, **new_tags):
    old_tags = {}

    if update:
        try:
            old = s3_client.get_object_tagging(Bucket=bucket, key=object)
            old_tags = {i['Key']: i['Value'] for i in old['TagSet']}
        except Exception as e:
            print(e)
            print("There was no tag")

    new_tags = {**old_tags, **new_tags}

    response = s3_client.put_object_tagging(
        Bucket=bucket,
        Key=object,
        Tagging={
            'TagSet': [{'Key': str(k), 'Value': str(v)} for k, v in new_tags.items()]
        }
    )

    print(response)


def delete_bucket_objects(Name, type, age, FileSize):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            object = s3_resource.ObjectSummary(Name, my_bucket_object.key)

            if object.size > FileSize and object.key[-3:] == type and object.last_modified.replace(tzinfo=None).isoformat() < age:
                # print(object.last_modified - timedelta(days=age) )
                print(object.key, object.size,
                      object.last_modified, object.key[-3:])
                logging.info(f'Bucket = {Name}:Deleing object {object.key} from {Name} ')
                object.delete()
    except ClientError as err:
        print(err.response['Error']['Code'])


def keep_bucket_objects(Name, StorageClass, type):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            # print(my_bucket_object)
            object = s3_resource.ObjectSummary(Name, my_bucket_object.key)
            # print(object.storage_class)
            # if object.storage_class != 'INTELLIGENT_TIERING' :
            if object.key[-3:] == type:
                print(object.key, object.storage_class)
                logging.info(f'Bucket = {Name}: Adding Tag to keep object {my_bucket_object.key}')
                Tagging = {
                    'TagSet': [
                        {
                            'Key': 'Retention',
                            'Value': 'Keep',
                        },
                    ],
                }
                set_object_tags(my_bucket, my_bucket_object, Tagging)
                # accepted values are 'STANDARD' |'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'
    except ClientError as err:
        print(err.response['Error']['Code'])


def modify_bucket_objects(Name, StorageClass, FileSize, type):
    try:
        my_bucket = s3_resource.Bucket(Name)
        for my_bucket_object in my_bucket.objects.all():
            # print(my_bucket_object)
            object = s3_resource.ObjectSummary(Name, my_bucket_object.key)
            # print(object.storage_class)
            # if object.storage_class != 'INTELLIGENT_TIERING' :
            if object.storage_class is None and object.size >= FileSize and object.key[-3:] != type:
                print(object.key, object.storage_class, object.size)
                logging.info(f'Bucket = {Name} : Changing Storage Class of {object.key} from  STANDARD to {StorageClass}')
                object.put(StorageClass=StorageClass)
                # accepted values are 'STANDARD' |'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'
    except ClientError as err:
        print(err.response['Error']['Code'])


def populate_data(config):
    for bucket in s3_client.buckets.all():
        for item in config.sections():
            ignoreitem = config[item]['ignorebucket'].split(',')
            if bucket.name not in config and bucket.name not in ignoreitem:
                logging.info(f'Adding Bucket = {bucket.name} in Config.ini file')
                config[bucket.name] = {}
                with open(file, 'w') as configfile:
                    config.write(configfile)


if __name__ == '__main__':
    logging.info(f'{__file__}')
    # visit_buckets()
    # config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
    config = ConfigParser()
    config.read(file)
    populate_data(config)
    for bucket in config.sections():
        print(bucket)
        StorageClass = config[bucket]['StorageClass'].upper().replace("'", "")
        # FileType = config.getlist[bucket]['FileType']
        FileType = [e.strip() for e in config.get(bucket, 'FileType').split(',')]
        FileSize = config[bucket]['FileSize']
        print(StorageClass)
        print(FileSize)
        for ty1 in FileType:
            print(ty1)
            type = ty1.split(':')[0]
            action = ty1.split(':')[1]
            if action.lower() == 'delete':
                aget = int(ty1.split(':')[2])
                age = (datetime.today().replace(tzinfo=None) -
                       timedelta(days=aget)).isoformat()
                print(age)
                delete_bucket_objects(bucket, type, age, int(float(FileSize)))
            elif action.lower() == 'keep':
                keep_bucket_objects(bucket, StorageClass, type)
            elif action.lower() == 'ignore':
                logging.info(
                    f'Ignoring {type} type of objects in Bucket = {bucket}')
            else:
                modify_bucket_objects(
                    bucket, StorageClass, int(float(FileSize)), type)
