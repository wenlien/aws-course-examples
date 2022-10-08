import os

import boto3
from dotenv import load_dotenv

load_dotenv()


def _get_boto_session(profile_name=None):
    profile_name = profile_name or os.getenv('AWS_PROFILE') or 'default'
    return boto3.session.Session(profile_name=profile_name)


def _get_boto_resource(resource, profile_name=None):
    return _get_boto_session(profile_name).resource(resource)
    # return boto3.resource(resource)  # use profile of 'AWS_PROFILE' env or 'default'


def _get_boto_client(resource, profile_name=None):
    return _get_boto_session(profile_name).client(resource)
    # return boto3.client(resource)  # use profile of 'AWS_PROFILE' env or 'default'


def _change_profile_of_default_session(profile_name):
    boto3.setup_default_session(profile_name=profile_name)


def _get_aws_account_id():
    return os.getenv('aws_account_id', 'stanley')


def _get_default_region():
    return os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-1')


def _get_aws_doc_lang():
    return os.getenv('aws_doc_lang', 'zh_tw')


def _get_awsscripts_dir():
    return os.getenv('awsscripts_dir', f'{os.path.expanduser("~")}/awsscripts')


def _is_ec2_instance():
    import requests
    from requests.exceptions import ConnectTimeout
    try:
        _ = requests.get('http://169.254.169.254/latest/meta-data/hostname', timeout=1)
    except ConnectTimeout as ex:
        return False
    return True


def open_ref_url():
    for url in os.environ['ref_url'].split(';'):
        os.system(f"open {url}")

