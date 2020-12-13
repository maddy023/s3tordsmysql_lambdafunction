import json
import boto3
import pymysql 

s3_cient = boto3.client('s3')
def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)

    data = resp['Body'].read().decode('utf-8')
    data = data.split("\n")

    rds_endpoint  = "s3tords.cisv6nrihcgq.ap-south-1.rds.amazonaws.com"
    username = "" # RDS Mysql username
    password = "" # RDS Mysql password
    db_name = "" # RDS MySQL DB name
    conn = None
    try:
        conn = pymysql.connect(rds_endpoint, user=username, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    try:
        cur = conn.cursor()
        cur.execute("create table s3databseori ( id INT NOT NULL AUTO_INCREMENT, Name varchar(255) NOT NULL, PRIMARY KEY (id))")
        conn.commit()
    except:
        pass
    
    with conn.cursor() as cur:
        for emp in data: # Iterate over S3 csv file content and insert into MySQL database
            try:
                emp = emp.replace("\n","").split(",")
                print (">>>>>>>"+str(emp))
                cur.execute('insert into s3databseori (Name) values("'+str(emp[1])+'")')
                conn.commit()
            except:
                continue
        cur.execute("select count(*) from s3databseori")
        
    print("Deleting the csv file from s3 bucket")
    try:
        response = s3_cient.delete_object(Bucket=bucket_name, Key=s3_file_name)
    except Exception as e:
        print(e)
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
