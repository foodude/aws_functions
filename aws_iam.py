import logging as log 
import boto3
from aws_session import create_session


log.getLogger().addHandler(log.NullHandler())


def list_users(**args):
    session = args.get('session', create_session())

    try:
       iam = session.resource('iam')
       return [x.name for x in iam.users.all()]

    except Exception as err:
        log.error({'args': args, 
                   'error': err})
        return False


def list_groups(**args):
    session = args.get('session', create_session())

    try:
       iam = session.resource('iam')
       return [x.name for x in iam.groups.all()]

    except Exception as err:
        log.error({'args': args, 
                   'error': err})
        return False


def create_user(**args):
    session = args.get('session', create_session())
    user_name = args.get('user_name')

    try:
        iam = session.client('iam')
        iam.create_user(
            UserName=user_name)
        return True

    except Exception as err:
        log.error({'args': args, 
                   'error': err})


def list_user_access_keys(**args):
    session = args.get('session', create_session())
    user_name = args.get('user_name')

    try:
        iam = session.client('iam')
        keys = iam.list_access_keys(
            UserName=user_name)
        return [x['AccessKeyId'] for x in keys['AccessKeyMetadata']]

    except Exception as err:
        log.error(err)
        return False


def create_user_access_key(**args):
    session = args.get('session', create_session())
    user_name = args.get('user_name')

    try:
        iam = session.client('iam')
        key_info = iam.create_access_key(
            UserName=user_name)['AccessKey']
        return {'aws_access_key_id': key_info['AccessKeyId'],
                'aws_secret_access_key': key_info['SecretAccessKey']}

    except Exception as err:
        log.error(err)
        return False


        

