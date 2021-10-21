# References
# code snippets picked up from given support guide
# https://www.tutorialspoint.com/python/python_command_line_arguments.htm
# https://stackoverflow.com/questions/8515053/csv-error-iterator-should-return-strings-not-bytes
import boto3
import sys
import csv

if len(sys.argv)>2:
    #initialising s3 and dynamodb resources
    s3 = boto3.resource('s3',aws_access_key_id=sys.argv[1],aws_secret_access_key=sys.argv[2])
    dyndb = boto3.resource('dynamodb',region_name='us-west-2',aws_access_key_id=sys.argv[1],aws_secret_access_key=sys.argv[2])
    
    #creating s3 bucket
    try:
        s3.create_bucket(Bucket='cloud-infra-adityadw', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    except Exception as error:
        print (error)
    
    print ("Done initialising s3 and dynamodb instances\n\n")
    #creating dynamodb table, here the Date column is the partition key and ID column is the row-key
    try:
        table = dyndb.create_table(
        TableName='cloud-infra-datatable',
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
    except Exception as e:
        print (e)
        #if there is an exception, the table may already exist and therefore retreive it
        table = dyndb.Table("cloud-infra-datatable")
    
    #wait for the table to be created
    table.meta.client.get_waiter('table_exists').wait(TableName='cloud-infra-datatable')
    print("Done creating Table\n\n")

    #making s3 bucket publicly readable
    bucket = s3.Bucket('cloud-infra-adityadw')
    bucket.Acl().put(ACL='public-read')

    counter=0
    #uploading blob file to s3 bucket, making it publicly visible, getting its public URL and creating meta-data record for each row in experiments.csv
    with open('experiments.csv', 'r') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        for item in csvf:
            #check if not the header row
            if counter!=0:
                print (item)
                body = open(item[5], 'rb')
                s3.Object('cloud-infra-adityadw', item[5]).put(Body=body )
                md = s3.Object('cloud-infra-adityadw', item[5]).Acl().put(ACL='public-read')

                url = "https://s3-us-west-2.amazonaws.com/cloud-infra-adityadw/"+item[5]
                metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                'Temp' : item[2], 'Conductivity' : item[3], 'Concentration' : item[4], 'url':url}
                try:
                    table.put_item(Item=metadata_item)
                except:
                    print ("item may already be there or another failure")
            counter=counter+1
    print("Done uploading data to table\n\n")

    
    #querying the table by fetching partition specific to a date using partition-key
    #and then getting relevant record from the partition using row-key
    response = table.get_item (
        Key = {
            'PartitionKey' : '10/20/2021',
            'RowKey' : '2'
        }
    )

    #printing the query result
    print("Query results are as follows")
    print(response['Item'])
    print("\n\n")

    #printing the entire response object
    print(response)
    
else:
    print ("Please supply key_id and access_key")