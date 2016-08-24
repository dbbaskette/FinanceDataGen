#from faker import Factory
from faker import Faker
import datetime, time, math
from datetime import timedelta
import csv,random
#from objects import Customer
import redis
import time
#import psycopg2
import queries
from logging.handlers import SysLogHandler
import logging
import requests


seedCustomerNumber = 40000





def addCustomerRedis(r,customerNum,customer):
    r.hset(customerNum,"firstName",customer[1])
    r.hset(customerNum, "lastName", customer[2])
    r.hset(customerNum, "address", customer[3])
    r.hset(customerNum, "city", customer[4])
    r.hset(customerNum, "state", customer[5])
    r.hset(customerNum, "zipCode", customer[6])
    r.hset(customerNum, "latitude", customer[7])
    r.hset(customerNum, "longitude", customer[8])
    r.hset(customerNum, "cardNum", customer[9])
    r.hset(customerNum, "phone", customer[10])
    r.hset(customerNum, "ssn", customer[11])
    r.hset(customerNum, "birthDate", customer[12])
    r.hset(customerNum, "age", customer[13])
    r.hset(customerNum, "email", customer[14])


def loadCustomerTable():
    print "Loading Customer Table in Database"
    dbURI = queries.uri("spiderman", port=5432, dbname="gpadmin", user="gpadmin", password="gpadmin")
    with queries.Session(dbURI) as session:
        result = session.query("drop table if exists customers;")
        result = session.query("create table customers(customerNum int,firstName text,lastName text,address text,city text,state char(2),zip int,latitude float,longitude float,cardNumber bigint,phone text,ssn varchar(11),birthDate date,age int,email text) with (appendonly=true, compresstype=snappy) DISTRIBUTED RANDOMLY;")
        with open('./data/customers.csv') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                rowString = "'"+"','".join(row)+"'"
                result = session.query("insert into customers VALUES ("+rowString+");")

def buildCustomer(sex,custNumber):
    fake = Faker()
    customer=[]
    customer.append(seedCustomerNumber+custNumber)
    if "m" in sex:
        firstName = fake.first_name_male()
        customer.append(firstName)

    else:
        firstName = fake.first_name_female()
        customer.append(firstName)

    lastName = fake.last_name()
    customer.append(lastName)
    customer.append(fake.street_address())
    city, state, zip, latitude, longitude = getAddress(0)
    customer.append(city)
    customer.append(state)
    customer.append(zip)
    customer.append(latitude)
    customer.append(longitude)
    customer.append(fake.credit_card_number())
    customer.append(fake.phone_number())
    customer.append(fake.ssn())

    birthDate = fake.date_time_between_dates(datetime_start=datetime.date(1920,1,1), datetime_end=datetime.datetime.now() - timedelta(days=math.trunc(18*365.2425)), tzinfo=None)
    ageTemp = datetime.datetime.now() - birthDate
    customer.append(datetime.datetime.date(birthDate))
    customer.append(math.trunc((ageTemp.days + ageTemp.seconds / 86400) / 365.2425))
    customer.append(firstName[0]+lastName+"@"+fake.free_email_domain())

    return customer

def outputCustomers(customers):

    with open("./data/customers.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(customers)


def addAddressesRedis():
    r = redis.Redis(host='localhost', port=6379, db=1)
    r.flushdb()
    with open('./data/zipcodes.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            r.hset(row[0], "city", row[2])
            r.hset(row[0], "state", row[3])
            r.hset(row[0], "latitude", row[5])
            r.hset(row[0], "longitude", row[6])





def getAddress(zipCode=0):
    r = redis.Redis(host='localhost', port=6379, db=1)
    if zipCode==0:
        zipCode = r.randomkey()
    city = r.hget(zipCode,"city")
    state = r.hget(zipCode,"state")
    latitude = r.hget(zipCode,"latitude")
    longitude = r.hget(zipCode,"longitude")
    return city,state,zipCode,latitude,longitude





def buildTransaction(numCustomers):
    fake = Faker()
    r0 = redis.Redis(host='localhost', port=6379, db=0)
    numCustomers = r0.dbsize()
    custNumber = seedCustomerNumber+random.randint(1,numCustomers)
    #custNumber = 40005
    customer = r0.hgetall(custNumber)
    transaction=[]
    transaction.append(custNumber)

    # 10% of the time generate a transaction away from home.
    transactionLocation = random.randint(1,11)
    if transactionLocation==1:
        city, state, zipCode, latitude, longitude = getAddress(0)
    else:
        zipCode = r0.hget(custNumber,"zipCode")
        city = r0.hget(custNumber,"city")
        state = r0.hget(custNumber, "state")
        latitude = r0.hget(custNumber, "latitude")
        longitude = r0.hget(custNumber, "longitude")

    #transaction.append(custNumber)
    transaction.append(city)
    transaction.append(state)
    transaction.append(zipCode)
    transaction.append(float(latitude))
    transaction.append(float(longitude))
    transaction.append(datetime.datetime.now())
    transaction.append(float(fake.numerify(transactionSize())))
    return transaction

def transactionSize():
    size = random.randint(1,11)
    if size==10:
        return "####.##"
    elif (size>6):
        return "###.##"
    elif (size>1):
        return "##.##"
    else :
        return "#.##"

def generateTransactions(numTransactions,numCustomers):
    print "GENERATE TRANSACTIONS"
    complete = False
    transactionCount=0

    while not complete:

        transaction = buildTransaction(numCustomers)
        print transaction[0]
        logging.info(transaction)
        r = requests.post("http://localhost:9000",data={transaction[0]:transaction})
        print r.text
        transactionCount+=1
        if transactionCount==numTransactions:
            complete=True

# CREATE TABLE AND WRITE THE CUSTOMERS TO THAT TABLE
# GENERATE TRANSACTION WILL THEN USE THAT AS BASE AND SHOW TRANSACTIONS FOR CUSTOMERS
# PHASE 1 will be generic and random transactions
#        Query Table to get a user and generate a transaction mostly in own territor
# FROM CUSTOMER NUMBER, LAT,LONG,CC#  then add itemdesc,category,amount,timestamp,companyname,lat,long



if __name__ == '__main__':

    logger = logging.getLogger()
    logger.addHandler(SysLogHandler(address=('localhost', 1514)))
    logger.setLevel(logging.INFO)


    numCustomers=10
    customers=[]
    addAddressesRedis()
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.flushdb()

    for x in range(1,numCustomers+1):
        customer = []
        sex="f"
        if x % 2 == 0:
           sex="m"
        customer = buildCustomer(sex,x)
        addCustomerRedis(r,customer[0],customer)
        customers.append(customer)
    outputCustomers(customers)



    #loadCustomerTable()
    generateTransactions(10, numCustomers)



    #outputCustomers(customers)
    #getCustomer()




