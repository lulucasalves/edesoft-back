import csv
import boto3
import json
import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


s3 = boto3.client('s3')
Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    phone = Column(String)
    cpf = Column(String)
    cnpj = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    birth_date = Column(DateTime)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


def lambda_handler(event, context):
    bucket_name = event['queryStringParameters']['bucket_name']
    object_key = event['queryStringParameters']['object_key']
    
    # download CSV file from S3 bucket
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    lines = response['Body'].read().decode('utf-8').split('\n')
    
    # read CSV file and insert data into MySQL database
    engine = create_engine('mysql+pymysql://user:password@hostname:port/database', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for line in csv.DictReader(lines):
        birth_date = datetime.datetime.strptime(line['birth_date'], '%d/%m/%Y').strftime('%Y-%m-%d')
        
        customer = Customer(
            name=line['name'],
            email=line['email'],
            phone=line['phone'],
            cpf=line['cpf'].replace('.', '').replace('-', ''),
            cnpj=line['cnpj'].replace('.', '').replace('/', '').replace('-', ''),
            address=line['address'],
            city=line['city'],
            state=line['state'],
            country=line['country'],
            birth_date=birth_date
        )
        
        session.add(customer)
    
    session.commit()
    
    response = {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data saved successfully.'}),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    
    return response
