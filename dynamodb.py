import copy
import os
import pdb
import random
import sys
import time

import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import STRING, NUMBER
from botocore.exceptions import WaiterError
import fire

from my_aws_py_base import _get_boto_client, _get_boto_resource


def get_default_table_name():
    return 'music-default'


def get_table_name():
    return 'music-test'


def _query_by_artist_songtitle(ddb_table, index_name, artist, song_title):
    response = ddb_table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('Artist').eq(artist)
    )
    return response['Items']


def _query_by_length_awards(ddb_table, index_name, lengths, awards):
    response = ddb_table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('Length').eq(lengths[0]) & Key('Awards').eq(awards[0])
    )
    return response['Items']


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def create_table(table_name=f'{get_table_name()}', delay=1, max_attempts=10):
    #ddb = _get_boto_resource('dynamodb')
    ddb = _get_boto_client('dynamodb')
    ddb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'Artist', 'KeyType': 'HASH'},
            {'AttributeName': 'SongTitle', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'Artist', 'AttributeType': STRING},
            {'AttributeName': 'SongTitle', 'AttributeType': STRING},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits':10,
            'WriteCapacityUnits':5,
        }
    )
    table_waiter(waiter_name='table_exists', table_name=table_name, delay=delay, max_attempts=max_attempts)
    print(f'Table ({table_name}) is created!')


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def delete_table(table_name=f'{get_table_name()}', delay=1, max_attempts=10):
    ddb = _get_boto_client('dynamodb')
    ddb.delete_table(TableName=table_name)
    table_waiter(waiter_name='table_not_exists', table_name=table_name, delay=delay, max_attempts=max_attempts)
    print(f'Table ({table_name}) is deleted!')


def recreate_table(table_name=f'{get_table_name()}', delay=None, max_attempts=None):
    delete_table(table_name, None, None)
    create_table(table_name, None, None)


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def put_item(table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    length = int(random.random() * 100)
    resp = table.put_item(
                Item = {
                    "Artist": "No One You Know",
                    "SongTitle": "Call Me Today",
                    "AlbumTitle": "Greatest Hits",
                    "Length": length,
                    "Awards": 1,
                }
            )
    print(resp)


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def batch_write(table_name=f'{get_table_name()}', amount=100):
    table = _get_boto_resource('dynamodb').Table(table_name)
    Item = {
        "Artist": "No One You Know-0",
        "SongTitle": "Call Me Today-0",
        "AlbumTitle": "Greatest Hits-0",
        "Length": 0,
        "Awards": 0,
    }
    with table.batch_writer() as batch:
        for i in range(1, amount+1):
            item = copy.deepcopy(Item)
            item['Artist'] = item['Artist'][0:-1] + str(i)
            item['SongTitle'] = item['SongTitle'][0:-1] + str(i)
            item['AlbumTitle'] = item['AlbumTitle'][0:-1] + str(i)
            item['Length']  = int(random.random() * 100)
            item['Awards']  = int(random.random() * 2)
            batch.put_item(Item=item)


# If the data type of the sort key is Number, the results are returned in numeric order;
# otherwise, the results are returned in order of UTF-8 bytes.
# By default, the sort order is ascending.
def query_GSI_top_N_items(table_name=f'{get_default_table_name()}', top=3, asc=True):
    table = _get_boto_resource('dynamodb').Table(table_name)
    index_name = 'AlbumTitle-Length-index'
    print(f'Table Name={table_name}, Top={top}, Ascending={asc}: ')
    resp = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('AlbumTitle').eq('Album Title'),
                Limit=top,
                ScanIndexForward=asc
            )
    for item in resp['Items']:
        print(item['Artist'], item['Length'])


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def get_item(idx=1, table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    resp = table.get_item(
                Key={
                        'Artist': 'No One You Know' if idx is None else f'No One You Know-{idx}',
                        'SongTitle': 'Call Me Today' if idx is None else f'Call Me Today-{idx}',
                    }
            )
    print(resp['Item'])


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def scan_table(table_name=f'{get_table_name()}'):
    from boto3.dynamodb.conditions import Key, Attr
    table = _get_boto_resource('dynamodb').Table(table_name)
    resp1 = table.scan()
    resp2 = table.scan(FilterExpression=Attr('Length').eq(1) & Attr('Awards').eq(1))
    print(f'''
Full table scan:
{'-' * 24}
{resp1['Items']}
{'=' * 24}
The query returned the following items:
{'-' * 24}
{resp2['Items']}
''')


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def update_item(idx=None, value=99, table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    resp = table.update_item(
                Key={
                    'Artist': 'No One You Know' if idx is None else f'No One You Know-{idx}',
                    'SongTitle': 'Call Me Today' if idx is None else f'Call Me Today-{idx}',
                },
                ExpressionAttributeNames={
                    "#length": 'Length',
                    "#awards": 'Awards',
                },
                ExpressionAttributeValues={
                    ':length': value,
                    ':awards': value,
                },
                UpdateExpression='SET #length = :length, #awards = :awards',
            )
    print(resp)


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def delete_item(idx=None, table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    resp = table.delete_item(
                Key={
                    'Artist': 'No One You Know' if idx is None else f'No One You Know-{idx}',
                    'SongTitle': 'Call Me Today' if idx is None else f'Call Me Today-{idx}',
                },
            )
    print(resp)


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def query_table(table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    #resp = table.query(KeyConditionExpression=Key('Artist').eq('No One You Know-1') & Key('SongTitle').eq('Call Me Today-1'))
    resp = table.query(KeyConditionExpression=Key('Artist').eq('No One You Know-1') & Key('SongTitle').begins_with('Call Me Today'))
    print(f'''
Query: KeyConditionExpression=Key('Artist').eq('No One You Know') & Key('SongTitle').eq('Call Me Today')
The query returned the following items:
{'-' * 24}
{resp['Items']}
''')


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def create_global_secondary_index(index_name=None, table_name=f'{get_table_name()}'):
    ddb = _get_boto_client('dynamodb')
    attr_name = 'Length'
    attr_type = NUMBER
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    resp = ddb.update_table(
                TableName=table_name,
                AttributeDefinitions=[
                        {
                            'AttributeName': attr_name,
                            'AttributeType': attr_type,
                        }
                    ],
                GlobalSecondaryIndexUpdates=[
                        {
                            'Create': {
                                    'IndexName': index_name,
                                    'KeySchema': [
                                            {
                                                'AttributeName': attr_name,
                                                'KeyType': 'HASH',
                                            }
                                        ],
                                    'Projection': {
                                            'ProjectionType': 'ALL',
                                        },
                                    'ProvisionedThroughput': {
                                            'ReadCapacityUnits': 1,
                                            'WriteCapacityUnits': 1,
                                        }
                                }
                        }
                    ]
            )
    print('Secondary index is creating!')
    print(resp)


def delete_global_secondary_index(index_name=None, table_name=f'{get_table_name()}'):
    ddb = _get_boto_client('dynamodb')
    attr_name = 'Length'
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    resp = ddb.update_table(
                TableName=table_name,
                GlobalSecondaryIndexUpdates=[
                        {'Delete': {'IndexName': index_name}}
                     ]
            )
    print('Global Secondary Index is deleting!')
    print(resp)


def __checking_global_secondary_index(table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    time.sleep(5)
    while True:
        if table.global_secondary_indexes and table.global_secondary_indexes[0]['IndexStatus'] == 'ACTIVE':
            print(f'Global Secondary Index ({table.global_secondary_indexes[0]["IndexName"]}) is created!')
            break
        print('Waiting for index to backfill...')
        time.sleep(5)
        table.reload()


def check_global_secondary_index(status=None, index_name=None, table_name=f'{get_table_name()}', table=None):
    valid_status = ['active', 'exists', 'not_exists']
    assert(status in valid_status and index_name and (table_name or table))
    table = table or _get_boto_resource('dynamodb').Table(table_name)
    if status == 'active':
        return any(gsi for gsi in table.global_secondary_indexes or [] if gsi.get('IndexName') == index_name and gsi.get('IndexStatus') == 'ACTIVE')
    if status == 'exists':
        return any(gsi for gsi in table.global_secondary_indexes or [] if gsi.get('IndexName') == index_name)
    if status == 'not_exists':
        return not any(gsi for gsi in table.global_secondary_indexes or [] if gsi.get('IndexName') == index_name)


def show_global_secondary_index_status(index_name=None, table_name=f'{get_table_name()}', table=None):
    attr_name = 'Length'
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    table = table or _get_boto_resource('dynamodb').Table(table_name)
    while True:
        indexes = [gsi for gsi in table.global_secondary_indexes if gsi.get('IndexName') == index_name]
        if not len(indexes):
            return
        index = indexes[0]
        print(f'Index status: {index.get("IndexStatus")}')
        time.sleep(5)
        table.reload()


def get_global_secondary_index_status(index_name=None, table_name=f'{get_table_name()}', table=None):
    attr_name = 'Length'
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    table = table or _get_boto_resource('dynamodb').Table(table_name)
    indexes = [gsi for gsi in table.global_secondary_indexes if gsi.get('IndexName') == index_name]
    if not indexes:
        return None
    return indexes[0].get('IndexStatus')


def wait_global_secondary_index(waiter=None, index_name=None, table_name=f'{get_table_name()}'):
    valid_waiters = ['active', 'exists', 'not_exists']
    assert(waiter in valid_waiters and index_name and table_name)
    table = _get_boto_resource('dynamodb').Table(table_name)
    while True:
        if check_global_secondary_index(waiter, index_name, table_name, table):
            break
        #status = get_global_secondary_index_status(index_name, table_name, table)
        #print(f'Wait GSI ({index_name}) being {waiter}...')
        print(f'Wait GSI ({index_name}) being {waiter}... ({get_global_secondary_index_status(index_name, table_name, table)})')
        time.sleep(5)
        table.reload()


def recreate_global_secondary_index(index_name=None, table_name=f'{get_table_name()}'):
    attr_name = 'Length'
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    if check_global_secondary_index('exists', index_name, table_name):
        delete_global_secondary_index(index_name, table_name)
    wait_global_secondary_index('not_exists', index_name, table_name)
    create_global_secondary_index(index_name, table_name)
    wait_global_secondary_index('active', index_name, table_name)


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def query_global_secondary_index(index_name=None, table_name=f'{get_table_name()}'):
    table = _get_boto_resource('dynamodb').Table(table_name)
    attr_name = 'Length'
    index_name = index_name or table_name + '-' + attr_name + '-global-secondary-index'
    #pdb.set_trace()

    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(5)
            table.reload()
        else:
            break

    resp = table.query(
            IndexName=index_name,
            KeyConditionExpression=Key(attr_name).eq(1),
        )

    print(f'''
Query: KeyConditionExpression=Key("{attr_name}").eq(1)
The query returned the following items:
{"-" * 24}
{resp["Items"]}
''')


# https://hands-on.cloud/working-with-dynamodb-in-python-using-boto3/#h-connecting-to-dynamodb-apis-using-boto3
def backup_table(table_name=f'{get_table_name()}', backup_table_name=f'{get_table_name()}-backup'):
    ddb = _get_boto_client('dynamodb')
    resp = ddb.create_backup(
                TableName=table_name,
                BackupName=backup_table_name
            )
    print(resp)


def query(table_name=f'{get_default_table_name()}'):
    idx=10
    artist= f'Artist_{idx}'
    song_title = f'Song Title_{idx}'
    lengths=[10, 100]
    awards=[1, 10]
    index_name_1='Artist-SongTitle-index'
    index_name_2='Length-Awards-index'
    ddb_client = boto3.resource('dynamodb')
    ddb_table = ddb_client.Table(table_name)
    albums = _query_by_artist_songtitle(ddb_table, index_name_1, artist, song_title) or dict()
    if not albums:
        print(f'No data found!')
        SystemExit(0)
    for album in albums:
        print('|'.join(str(v) for v in album.values()))


def _helper_table_waiter(delay=1, max_attempts=10):
        print(f'''Usage:
    .pipenv_run.sh my_aws_py_dynamodb.py table_waiter [waiter] [table] [delay in second] [max attemps]
E.g.
    ./pipenv_run.sh my_aws_py_dynamodb.py table_waiter table_exists music-default {delay} {max_attempts}
Valid waiters:
    * table_exists
    * table_not_exists
''')


def table_waiter(waiter_name='table_exists', table_name=f'{get_default_table_name()}', delay=None, max_attempts=None):
    valid_waiters = ('table_exists', 'table_not_exists')

    if not waiter_name or not table_name:
        _helper_table_waiter()
        return

    if waiter_name not in valid_waiters:
        print(f'Waiter ({waiter_name}) is not valid! Valid waiters are {valid_waiters}.')
        return
    print(f'''
waiter name = {waiter_name}
table name = {table_name}
delay = {delay}
max attemps = {max_attempts}
''')
    waiter = _get_boto_client('dynamodb').get_waiter(waiter_name)
    if delay and max_attempts:
        waiter_config = {
                'Delay': delay,
                'MaxAttempts': max_attempts
            }
    else:
        waiter_config = {}

    try:
        waiter.wait(TableName=table_name, WaiterConfig=waiter_config)
    except WaiterError as ex:
        print('WaiterError is raised!')
        print(ex)
    except Exception as ex:
        print('Exception is raised!')
        print(ex)


def _get_transact_items(artist='default', song_title='default', length=None, is_update=False):
    length = length if length else int(random.random() * 100)
    returns = [
        {
            'Put': {
                'TableName': 'music-default',
                'Item': {
                    'Artist': { 'S': f'USER#{artist}' },
                    'SongTitle': { 'S': f'SONG#{song_title}' },
                    'Length': { 'N': str(length) },
                },
                # 'ConditionExpression': 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'
            }
        },
        {
            'Update': {
                'TableName': 'music-test',
                'Item': {
                    'Artist': { 'S': f'USER#{artist}' },
                    'SongTitle': { 'S': f'SONG#{song_title}' },
                },
                # 'ConditionExpression': 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'
            }
        }
    ]

    if not is_update:
        returns[0]['Put']['ConditionExpression'] = 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'
        returns[1]['Put']['ConditionExpression'] = 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'

    print(f'Transact Items = {returns}')
    return returns

def _get_transact_items_for_update(artist='default', song_title='default', length=None):
    '''
    botocore.exceptions.ClientError:
    An error occurred (ValidationException) when calling the TransactWriteItems operation:
    Transaction request cannot include multiple operations on one item
    '''

    length = length if length else int(random.random() * 100)
    returns = [
        {
            'ConditionCheck': {
                'TableName': 'music-test',
                'Key': {
                    'Artist': { 'S': f'USER#{artist}' },
                    'SongTitle': { 'S': f'Song#{song_title}' },
                },
                'ConditionExpression': 'attribute_exists(#Artist)',
                #'ConditionExpression': 'attribute_not_exists(#Artist)',
                'ExpressionAttributeNames': {
                    '#Artist': 'Artist'
                },
            }
        },
        {
            'Put': {
                'TableName': 'music-test',
                'Item': {
                    'Artist': { 'S': f'USER#{artist}2' },
                    'SongTitle': { 'S': f'Song#{song_title}2' },
                    'Length': { 'N': str(length) },
                },
            }
        }
    ]
]
    print(f'Transact Items={returns}')
    return returns

def transact_write_create_new_user(length=None):
    length = length if length else int(random.random() * 100)
    ddb = _get_boto_client('dynamodb')
    response = ddb.transact_write_items(TransactItems=_get_transact_items(length=length))


def transact_write_update_new_user(length=None):
    length = length if length else int(random.random() * 100)
    ddb = _get_boto_client('dynamodb')
    response = ddb.transact_write_items(TransactItems=_get_transact_items(length=length, is_update=True))


def transact_write_update_user(length=None):
    length = length if length else int(random.random() * 100)
    ddb = _get_boto_client('dynamodb')
    response = ddb.transact_write_items(TransactItems=_get_transact_items_for_update(length=length))


if __name__ == '__main__':
    run_at = os.path.dirname(sys.argv[0])
    fire.Fire()

