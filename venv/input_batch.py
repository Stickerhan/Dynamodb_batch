import boto3
import time
import json
import decimal
import random
from boto3.dynamodb.conditions import Key, Attr
# Get the service resource.
hpcdynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

table = hpcdynamodb.Table('hpcBatchInput')
table_exits = False

try:
    table.creation_date_time
    table_exists = True
except:
    table_exists = False

if table_exists:
    table = hpcdynamodb.Table('hpcBatchInput')
else:
# Create the DynamoDB table.
    table = hpcdynamodb.create_table(
    TableName='hpcBatchInput',
    KeySchema=[
        {
            'AttributeName': 'unix_time',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'sort_name',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'unix_time',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'sort_name',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }

)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='hpcBatchInput')
# Using the exiting table.

table = hpcdynamodb.Table('hpcBatchInput')

# Print the table status
print("Table status:", table.table_status)
print(table.creation_date_time)

#Input and query test
value_list = []
sort_list = []
for i in range(10):
    unix = str(random.randint(1, 2))
    sort = str(random.randint(1, 100))
    value_list.append(unix)
    sort_list.append(sort)
    table.put_item(
        Item= {
            'unix_time': unix,
            'sort_name': sort
        }
    )
    response = table.query(
        KeyConditionExpression=Key('unix_time').eq('1')
    )
    items = response['Items']
    print(items)

# #Load data
# with open("/Users/lihan/Downloads/PythonProject/Dynamodb_batch/venv/hpc_query.json") as json_file:
#     hpc_data = json.load(json_file, parse_float = decimal.Decimal)
#     for query_data in hpc_data:
#         unix_time = int(query_data['year'])
#         sort_name = query_data['title']
#
#         print("Adding data:", unix_time, sort_name)
#
#         table.put_item(
#            Item={
#                'unix_time': unix_time,
#                'sort_name': sort_name,
#             }
#         )
