import logging as log 
import boto3
import botocore


log.getLogger().addHandler(log.NullHandler())


def create_session(**args):
    profile_name = args.get('profile_name', 'default')

    try:
        return boto3.Session(profile_name=profile_name)

    except botocore.exceptions.ProfileNotFound as err:
        log.error(err)
        return False

