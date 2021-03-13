import boto3

s3 = boto3.resource('s3',
	aws_access_key_id='REDACTED',
	aws_secret_access_key='REDACTED'
)

try:
	s3.create_bucket(Bucket='bal-timsina-bucket-1-69', CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'})
except:
	print "this may already exist"

bucket = s3.Bucket("bal-timsina-bucket-1-69")

bucket.Acl().put(ACL='public-read')

body = open('/Users/kristimsina/Desktop/Homework_2/experiments.csv', 'rb')

o = s3.Object('bal-timsina-bucket-1-69', 'test').put(Body=body)

s3.Object('bal-timsina-bucket-1-69', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
	region_name='us-west-2',
	aws_access_key_id='REDACTED',
	aws_secret_access_key='REDACTED'
)

try:
	table = dyndb.create_table(
		TableName='DataTable',
		KeySchema=[
			{
				'AttributeName': 'PartitionKey',
				'KeyType': 'HASH'
			},
			{
				'AttributeName': 'RowKey',
				'KeyType': 'RANGE'
			}
		],
		AttributeDefinitions=[
			{
				'AttributeName': 'PartitionKey',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'RowKey',
				'AttributeType': 'S'
			},
		],
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5
		}
	)
except:
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv

with open('/Users/kristimsina/Desktop/Homework_2/experiments.csv', 'r', encoding='utf-8') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	next(csvf, None)
	for item in csvf:
		print(item)
		body = open('/Users/kristimsina/Desktop/Homework_2/datafiles//'+item[3], 'rb')
		s3.Object('bal-timsina-bucket-1-69', item[3]).put(Body=body )
		md = s3.Object('bal-timsina-bucket-1-69', item[3]).Acl().put(ACL='public-read')

		url = "https://s3-us-west-2.amazonaws.com/bal-timsina-bucket-1-69/"+item[3]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
		'description' : item[4], 'date' : item[2], 'url':url}
		try:
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")


response = table.get_item(
	Key={
		'PartitionKey': 'experiment1',
		'RowKey': '1'
	}
)
item = response['Item']
print(item)