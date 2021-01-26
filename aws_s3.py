import logging as log
import uuid
import boto3
import botocore
from aws_session import create_session


def list_buckets(**args):
    session = args.get('session', create_session())

    try:
        s3 = session.resource('s3')
        return [x.name for x in s3.buckets.all()]

    except Exception as err:
        log.error(err)
        return False


def create_bucket(**args):
    session = args.get('session', create_session())
    bucket_prefix = args.get('bucket_prefix', str(uuid.uuid4()))
    bucket_name = args.get('bucket_name', 'unnamed')

    try:
        region_name = session.region_name
        s3 = session.resource('s3')
        s3.create_bucket(
            Bucket=f'{bucket_name}-{bucket_prefix}',
            CreateBucketConfiguration={
                'LocationConstraint': region_name})
          
        log.info('S3 bucket created: %s %s', 
            region_name, 
            bucket_name)
        return True
        
    except Exception as err:
        log.error(err)
        return False


def delete_bucket(**args):
    session = args.get('session', create_session())
    bucket_name = args.get('bucket_name')

    try:
        s3 = session.resource('s3')
        s3.Bucket(bucket_name).delete()
        log.info('S3 bucket deleted: %s', bucket_name)
        return True

    except Exception as err:
        log.error(err)
        return False


def list_files(**args):
    session = args.get('session', create_session())
    bucket_name = args.get('bucket_name')
    profile_name = args.get('profile_name', 'default')

    try:
        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        return [x.key for x in bucket.objects.all()]

    except Exception as err:
        log.error({'args': args, 
                   'error': err})
        return False


def upload_file(**args):
    session = args.get('session', create_session())
    file_name = args.get('file_name')
    bucket_name = args.get('bucket_name')

    try:
        s3 = session.client('s3')
        s3.upload_file(
            file_name,
            bucket_name,
            file_name)

    except Exception as err:
        log.error({'args': args, 
                   'error': err})
        return False


def dowload_file(**args):
    session = args.get('session', create_session())
    bucket_name = args.get('bucket_name')
    file_name = args.get('file_name')
    destination = args.get('destination', file_name)

    try:
        s3 = session.resource('s3')
        s3.Bucket(bucket_name).download_file(
            file_name,
            destination)
    
    except Exception as err:
        log.error(err)
        return False


def bucket_info(**args):
    session = args.get('session', create_session())
    bucket_name = args.get('bucket_name')

    try:
        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        size_list = [x.size for x in bucket.objects.all()]
        region_name = s3.meta.client.get_bucket_location(
            Bucket=bucket_name)['LocationConstraint']

        return {'bucket_name': bucket_name,
                'bucket_size_b': sum(size_list),
                'number_of_files': len(size_list),
                'region_name': region_name}

    except Exception as err:
        log.error(err)
        return False
