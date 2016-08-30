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
import os
from dotenv import load_dotenv
import json
import argparse
import numpy as np
import scipy.stats as stats
import random



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
    r.hset(customerNum, "age", customer[12])
    r.hset(customerNum, "email", customer[13])
    r.hset(customerNum, "job", customer[14])
    r.hset(customerNum, "creditability", customer[15])
    r.hset(customerNum, "accountBalanceStatus", customer[16])
    r.hset(customerNum, "creditDuration", customer[17])
    r.hset(customerNum, "paymentStatusPrevCredit", customer[18])
    r.hset(customerNum, "purpose", customer[19])
    r.hset(customerNum, "creditAmount", customer[20])
    r.hset(customerNum, "savingsValue", customer[21])
    r.hset(customerNum, "employmentLength", customer[22])
    r.hset(customerNum, "creditPercent", customer[23])
    r.hset(customerNum, "sexMaritalStatus", customer[24])
    r.hset(customerNum, "guarantors", customer[25])
    r.hset(customerNum, "durationAddess", customer[26])
    r.hset(customerNum, "mostValAsset", customer[27])
    r.hset(customerNum, "existingLines", customer[28])
    r.hset(customerNum, "typeResidence", customer[29])
    r.hset(customerNum, "existingLinesBank", customer[30])
    r.hset(customerNum, "employmentType", customer[31])
    r.hset(customerNum, "dependendents", customer[32])
    r.hset(customerNum, "telephoneAvail", customer[33])
    r.hset(customerNum, "foreignWorker", customer[34])
    r.hset(customerNum, "training", np.random.randint(0,1))


def loadCustomerTable():
    print "Loading Customer Table in Database"
    dbURI = queries.uri(os.environ.get("DBHOST"), port=os.environ.get("DBPORT"), dbname="gpadmin", user="gpadmin", password="gpadmin")
    with queries.Session(dbURI) as session:
        result = session.query("drop table if exists customers CASCADE ;")
        #result = session.query("create table customers(customerNum int,firstName text,lastName text,address text,city text,state char(2),zip int,latitude float,longitude float,cardNumber bigint,phone text,ssn varchar(11),age int,email text,sex char,job text,married smallint,balance float) with (appendonly=true, compresstype=snappy) DISTRIBUTED RANDOMLY;")
        #result = session.query("create table customers(customerNum int,firstName text,lastName text,address text,city text,state char(2),zip int,latitude float,longitude float,cardNumber bigint,phone text,ssn varchar(11),age int,email text,sex char,job text,married smallint,balance float,accountStatus text,accountDuration int,creditHistory text,purpose text,savingsBalance float,employmentStatus text,creditPercentage int,otherDebtors text,presentResidenceSince int, property text,otherInstallmentCedit text,otherCredit int,employmentType text,dependents int, telephoneAvail int,foreignWorker int) with (appendonly=true, compresstype=snappy) DISTRIBUTED RANDOMLY;")
        result = session.query("create table customers(customerNum int,firstName text,lastName text,address text,city text,state char(2),zip int,latitude float,longitude float,cardNumber bigint,phone text,ssn varchar(11),age int,email text,job text,"
                               "creditability int,accountBalanceStatus int,creditDuration int,paymentStatusPrevCredit int,purpose int,creditAmount int,"
                               "savingsValue int,employmentLength int,creditPercent int,sexMaritalStatus int, guarantors int,durationAddess int,"
                               "mostValAsset int,existingLines int,typeResidence int,existingLinesBank int,employmentType int,dependents int,telephoneAvail int,foreignWorker int,training int) with (appendonly=true) DISTRIBUTED RANDOMLY;")

        with open('./data/customers.csv') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                rowString = "'"+"','".join(row)+"'"
                result = session.query("insert into customers VALUES ("+rowString+");")

def buildTransactionTables():
    print "Adding Tables and Views in Database"
    dbURI = queries.uri(os.environ.get("DBHOST"), port=os.environ.get("DBPORT"), dbname="gpadmin", user="gpadmin",
                        password="gpadmin")
    with queries.Session(dbURI) as session:
        result = session.query("drop table if exists transactions CASCADE ;")
        result = session.query("drop external table if exists transactions_pxf CASCADE ;")
        result = session.query("drop view if exists suspect CASCADE ;")

        result = session.query("create table transactions(customernum integer,city text,state char(2),zip integer,latitude float,longitude float,time timestamp,amount float) with (appendonly=true) DISTRIBUTED RANDOMLY;")
        result = session.query("create external table transactions_pxf(like transactions) LOCATION('pxf://"+os.environ.get("DBHOST")+":51200/scdf/*.txt?PROFILE=HDFSTextSimple') FORMAT 'CSV'  LOG ERRORS INTO err_transactions SEGMENT REJECT LIMIT 5;")
        result = session.query("insert into transactions (select * from transactions_pxf);")
        result = session.query("create view suspect_view as (SELECT c.latitude AS clat, c.longitude AS clong, t.latitude AS tlat, t.longitude AS tlong, t.amount FROM transactions t, customers c WHERE c.customernum = t.customernum AND c.latitude <> t.latitude AND c.longitude <> t.longitude);")

def loadBankingData():
    r = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=3)
    with open('./data/german_credit.csv') as csvfile:
        reader = csv.reader(csvfile)
        rowNumber = 0
        for parsedRow in reader:
            # 21 Attributes
            r.hset(rowNumber, "creditability", parsedRow[0])
            r.hset(rowNumber, "accountBalanceStatus", parsedRow[1])
            r.hset(rowNumber, "creditDuration", parsedRow[2])
            r.hset(rowNumber, "paymentStatusPrevCredit", parsedRow[3])
            r.hset(rowNumber, "purpose", parsedRow[4])
            r.hset(rowNumber, "creditAmount", parsedRow[5])
            r.hset(rowNumber, "savingsValue", parsedRow[6])
            r.hset(rowNumber, "employmentLength", parsedRow[7])
            r.hset(rowNumber, "creditPercent", parsedRow[8])
            r.hset(rowNumber, "sexMaritalStatus", parsedRow[9])
            r.hset(rowNumber, "guarantors", parsedRow[10])
            r.hset(rowNumber, "durationAddess", parsedRow[11])
            r.hset(rowNumber, "mostValAsset", parsedRow[12])
            r.hset(rowNumber, "age", parsedRow[13])
            r.hset(rowNumber, "existingLines", parsedRow[14])
            r.hset(rowNumber, "typeResidence", parsedRow[15])
            r.hset(rowNumber, "existingLinesBank", parsedRow[16])
            r.hset(rowNumber, "employmentType", parsedRow[17])
            r.hset(rowNumber, "dependendents", parsedRow[18])
            r.hset(rowNumber, "telephoneAvail", parsedRow[19])
            r.hset(rowNumber, "foreignWorker", parsedRow[20])



            # if parsedRow[0].strip()=="A11":
            #     r.hset(rowNumber,"accountStatus",1)
            # elif parsedRow[0].strip()=="A12":
            #     r.hset(rowNumber,"accountStatus",2)
            # elif parsedRow[0].strip()=="A13":
            #     r.hset(rowNumber,"accountStatus",3)
            # elif parsedRow[0].strip()=="A14":
            #     r.hset(rowNumber,"accountStatus",4)
            # r.hset(rowNumber, "accountDuration", parsedRow[1].strip())
            #
            # #r.hset(rowNumber, "creditHistory", parsedRow[2].strip())
            # if parsedRow[2].strip() == "A30":
            #     r.hset(rowNumber, "creditHistory", 1)
            # elif parsedRow[2].strip() == "A31":
            #     r.hset(rowNumber, "creditHistory", 2)
            # elif parsedRow[2].strip() == "A32":
            #     r.hset(rowNumber, "creditHistory", 3)
            # elif parsedRow[2].strip() == "A33":
            #     r.hset(rowNumber, "creditHistory", 4)
            # elif parsedRow[2].strip() == "A34":
            #     r.hset(rowNumber, "creditHistory", 5)
            #
            #
            #
            #
            # #r.hset(rowNumber, "purpose", parsedRow[3].strip())
            # if parsedRow[3].strip() == "A40":
            #     r.hset(rowNumber, "purpose", 1)
            # elif parsedRow[3].strip() == "A41":
            #     r.hset(rowNumber, "purpose", 2)
            # elif parsedRow[3].strip() == "A42":
            #     r.hset(rowNumber, "purpose", 3)
            # elif parsedRow[3].strip() == "A43":
            #     r.hset(rowNumber, "purpose", 4)
            # elif parsedRow[3].strip() == "A44":
            #     r.hset(rowNumber, "purpose", 5)
            # elif parsedRow[3].strip() == "A45":
            #     r.hset(rowNumber, "purpose", 6)
            # elif parsedRow[3].strip() == "A46":
            #     r.hset(rowNumber, "purpose", 7)
            # elif parsedRow[3].strip() == "A47":
            #     r.hset(rowNumber, "purpose", 8)
            # elif parsedRow[3].strip() == "A48":
            #     r.hset(rowNumber, "purpose", 9)
            # elif parsedRow[3].strip() == "A49":
            #     r.hset(rowNumber, "purpose", 10)
            # elif parsedRow[3].strip() == "A410":
            #     r.hset(rowNumber, "purpose", 11)
            #
            # r.hset(rowNumber, "creditBalance", int(parsedRow[4].strip())+np.round(np.random.randint(1,99)/100.00,2))
            #
            #
            # #r.hset(rowNumber, "savingsBalance", parsedRow[5].strip())
            # change = np.round(np.random.randint(1,99)/100.00,2)
            #
            #
            # if parsedRow[5].strip()=="A61":
            #     r.hset(rowNumber,"savingsBalance",np.random.randint(1,100)+change)
            # elif parsedRow[5].strip()=="A62":
            #     r.hset(rowNumber,"savingsBalance",np.random.randint(100,500)+change)
            # elif parsedRow[5].strip()=="A63":
            #     r.hset(rowNumber,"savingsBalance",np.random.randint(500,1000)+change)
            # elif parsedRow[5].strip()=="A64":
            #     r.hset(rowNumber,"savingsBalance",np.random.randint(1000,10000)+change)
            # elif parsedRow[5].strip() == "A65":
            #     r.hset(rowNumber, "savingsBalance", 0.00)
            #
            #
            #
            #
            # if parsedRow[6].strip()=="A71":
            #     r.hset(rowNumber, "employmentStatus", "0")
            # elif parsedRow[6].strip()=="A72":
            #     r.hset(rowNumber, "employmentStatus", "1")
            # elif parsedRow[6].strip() == "A73":
            #     r.hset(rowNumber, "employmentStatus", np.random.randint(1,4))
            # elif parsedRow[6].strip() == "A74":
            #     r.hset(rowNumber, "employmentStatus", np.random.randint(4,7))
            # elif parsedRow[6].strip() == "A75":
            #     r.hset(rowNumber, "employmentStatus", np.random.randint(7,30))
            #
            #
            # r.hset(rowNumber, "creditPercentage", parsedRow[7].strip())
            # # Parse this into 2
            #
            # if parsedRow[8].strip()=="A91":
            #     r.hset(rowNumber, "sex", "0")
            #     r.hset(rowNumber, "maritalStatus", "2")
            # elif parsedRow[8].strip()=="A92":
            #     r.hset(rowNumber, "sex", "1")
            #     r.hset(rowNumber, "maritalStatus", "1")
            # elif parsedRow[8].strip()=="A93":
            #     r.hset(rowNumber, "sex", "0")
            #     r.hset(rowNumber, "maritalStatus", "0")
            # elif parsedRow[8].strip()=="A94":
            #     r.hset(rowNumber, "sex", "0")
            #     r.hset(rowNumber, "maritalStatus", "1")
            # elif parsedRow[8].strip()=="A95":
            #     r.hset(rowNumber, "sex", "1")
            #     r.hset(rowNumber, "maritalStatus", "0")
            #
            # if parsedRow[9].strip()=="A101":
            #     r.hset(rowNumber, "otherDebtors", "0")
            # elif parsedRow[9].strip()=="A102":
            #     r.hset(rowNumber, "otherDebtors", "1")
            # else:
            #     r.hset(rowNumber, "otherDebtors", "2")
            #
            #
            # r.hset(rowNumber, "presentResidenceSince", parsedRow[10].strip())
            # r.hset(rowNumber, "property", parsedRow[11].strip())
            # r.hset(rowNumber, "age", parsedRow[12].strip())
            # r.hset(rowNumber, "otherinstallmentCredit", parsedRow[13].strip())
            # r.hset(rowNumber, "housingStatus", parsedRow[14].strip())
            # r.hset(rowNumber, "otherCredit", parsedRow[15].strip())
            # r.hset(rowNumber, "employmentType", parsedRow[16].strip())
            # r.hset(rowNumber, "dependents", parsedRow[17].strip())
            # #r.hset(rowNumber, "telephoneAvail", parsedRow[18].strip())
            # if parsedRow[18].strip()=="A191":
            #     r.hset(rowNumber, "telephoneAvail", "0")
            # elif parsedRow[18].strip()=="A192":
            #     r.hset(rowNumber, "telephoneAvail", "1")
            # #r.hset(rowNumber, "foreignWorker", parsedRow[19].strip())
            # if parsedRow[19].strip()=="A201":
            #     r.hset(rowNumber, "foreignWorker", "0")
            # elif parsedRow[19].strip()=="A202":
            #     r.hset(rowNumber, "foreignWorker", "1")
            rowNumber+=1

def getBankingData(customerNumber):
    r = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=3)
    banking= {}


    # 21 Attributes
    banking["creditability"] = r.hget(customerNumber,"creditability")
    banking["accountBalanceStatus"] = r.hget(customerNumber,"accountBalanceStatus")
    banking["creditDuration"] = r.hget(customerNumber,"creditDuration")
    banking["paymentStatusPrevCredit"] = r.hget(customerNumber,"paymentStatusPrevCredit")
    banking["purpose"] = r.hget(customerNumber,"purpose")
    banking["creditAmount"] = r.hget(customerNumber,"creditAmount")
    banking["savingsValue"] = r.hget(customerNumber,"savingsValue")
    banking["employmentLength"] = r.hget(customerNumber,"employmentLength")
    banking["creditPercent"] = r.hget(customerNumber,"creditPercent")
    banking["sexMaritalStatus"] = r.hget(customerNumber,"sexMaritalStatus")
    banking["guarantors"] = r.hget(customerNumber,"guarantors")
    banking["durationAddess"] = r.hget(customerNumber,"durationAddess")
    banking["mostValAsset"] = r.hget(customerNumber,"mostValAsset")
    banking["age"] = r.hget(customerNumber,"age")
    banking["existingLines"] = r.hget(customerNumber,"existingLines")
    banking["typeResidence"] = r.hget(customerNumber,"typeResidence")
    banking["existingLinesBank"] = r.hget(customerNumber,"existingLinesBank")
    banking["employmentType"] = r.hget(customerNumber,"employmentType")
    banking["dependents"] = r.hget(customerNumber,"dependents")
    banking["telephoneAvail"] = r.hget(customerNumber,"telephoneAvail")
    banking["foreignWorker"] = r.hget(customerNumber,"foreignWorker")


    return banking


def buildCustomer(custNumber,age):
    fake = Faker()
    customer=[]
    customer.append(seedCustomerNumber+custNumber)
    if custNumber <= 999:
        banking=getBankingData(custNumber)
    else:
        banking=getBankingData(1000 - custNumber)

    if banking["sexMaritalStatus"]==0:
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

    #birthDate = fake.date_time_between_dates(datetime_start=datetime.date(1920,1,1), datetime_end=datetime.datetime.now() - timedelta(days=math.trunc(18*365.2425)), tzinfo=None)
    #ageTemp = datetime.datetime.now() - birthDate
    #customer.append(datetime.datetime.date(birthDate))

    # 14 Customer Attributes
    # 21 Banking Attributes
    customer.append(banking["age"])
    #customer.append(math.trunc((ageTemp.days + ageTemp.seconds / 86400) / 365.2425))
    customer.append(firstName[0]+lastName+"@"+fake.free_email_domain())
    customer.append((fake.job()).translate(None, "'"))

    customer.append(banking["creditability"])
    customer.append(banking["accountBalanceStatus"])
    customer.append(banking["creditDuration"])
    customer.append(banking["paymentStatusPrevCredit"])
    customer.append(banking["purpose"])
    customer.append(banking["creditAmount"])
    customer.append(banking["savingsValue"])
    customer.append(banking["employmentLength"])
    customer.append(banking["creditPercent"])
    customer.append(banking["sexMaritalStatus"])
    customer.append(banking["guarantors"])
    customer.append(banking["durationAddess"])
    customer.append(banking["mostValAsset"])
    customer.append(banking["existingLines"])
    customer.append(banking["typeResidence"])
    customer.append(banking["existingLinesBank"])
    customer.append(banking["employmentType"])
    customer.append(banking["dependents"])
    customer.append(banking["telephoneAvail"])
    customer.append(banking["foreignWorker"])

    return customer

def ageDistribution(numCustomers):

    maxAge = 98  # For a Nice Round Number
    minAge = 18  # For a Nice Round Number
    meanVal = (maxAge + minAge) / 2.0
    stdDev = 10        #23.0976
    s = stats.truncnorm((minAge - meanVal) / stdDev, (maxAge - meanVal) / stdDev, loc=meanVal, scale=stdDev)
    return np.round(s.rvs(numCustomers),0).astype(int)


def genderDistribution():
    if (np.random.choice([0, 1], size=1, p=[.494, .506]) == 0):
        return "m"
    else:
        return "f"


def outputCustomers(customers):

    with open("./data/customers.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(customers)


def addAddressesRedis():
    r = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=1)
    r.flushdb()
    with open('./data/zipcodes.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            r.hset(row[0], "city", row[2])
            r.hset(row[0], "state", row[3])
            r.hset(row[0], "latitude", row[5])
            r.hset(row[0], "longitude", row[6])





def getAddress(zipCode=0):
    r = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=1)
    if zipCode==0:
        zipCode = r.randomkey()
    city = r.hget(zipCode,"city")
    state = r.hget(zipCode,"state")
    latitude = r.hget(zipCode,"latitude")
    longitude = r.hget(zipCode,"longitude")
    return city,state,zipCode,latitude,longitude





def buildTransaction(transactionValue):

    fake = Faker()
    r0 = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=0)
    numCustomers = r0.dbsize()
    custNumber = seedCustomerNumber+random.randint(0,numCustomers-1)
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
    transaction.append(str(datetime.datetime.now()))
    transaction.append(float(transactionValue))
    return transaction


def transactionDistribution(numTransactions):
    transMax = 10001
    transMin = 1
    transMean = 100


    # Tried 100 different ways to get what I wanted, but settled on the easiest.
    # Used a triangular distribution from -4000 - 0 - 10000
    # Then take abs of the negative numbers which basically folds that portion
    # of the triangle ontop of the other positives.  This gives me additional
    # low transaction numbers which helps bring my mean down to a reasonable level.

    values = np.random.triangular(-4000, transMin, transMax, numTransactions)
    print np.max(values), np.min(values), np.mean(values)
    return np.round(np.abs(values),2)
    #stdDev = 1
    #numSD = 4

    #transQtyUni = transQty*.90
    #r1Mean = 50
    #r1 = ((r1Mean*2) - transMin) * np.random.random_sample(transQtyUni) + transMin
    #print np.max(r1), np.min(r1), np.mean(r1)
    # meanValue = actualMean / float(maxTransaction)/numSD
    # print "Mean Distribution",meanValue
    # s=np.abs(np.random.normal(meanValue, stdDev, numTransactions))
    # print "size",len(s)
    # w = s * (maxTransaction - actualMean) / numSD
    # print np.max(w),np.min(w),np.mean(w)
    #
    # # MANUAL DISTRIBUTION
    #
    # print  np.random.rand(0,1250)

    # first lets generate a block of random values around the mean (1 std dev)
    # a, b = (myclip_a - my_mean) / my_std, (myclip_b - my_mean) / my_std
    #  r = truncnorm.rvs(a, b, size=1000)

    #r = np.abs(np.random.normal(transMean/transQty, stdDev/transQty, transQty))
    #r = stats.truncnorm.rvs(1, transMean*2, size=100000)


    # ##r=r*      # Multiply by Total # Std Deviations to consider 6
    # print np.max(r), np.min(r), np.mean(r)
    # r = r*transMax
    # print np.max(r), np.min(r), np.mean(r)
    #
    # r1 = ((transMean) - transMin) * stats.truncnorm.rvs(1, transMean*2, size=100000) + transMin
    # print np.max(r1), np.min(r1), np.mean(r1)


    #rTotal = np.concatenate((r1,r2))
    #print np.max(rTotal), np.min(rTotal), np.mean(rTotal),len(rTotal)


def generateTransactions(numTransactions,numCustomers):
    print "GENERATE TRANSACTION"
    complete = False
    transactionCount=0
    transactionValues = transactionDistribution(numTransactions)
    while not complete:

        transaction = buildTransaction(transactionValues[transactionCount])
        #logging.info(transaction)
        postURL = "http://" + os.environ.get("POSTSERVER") + ":" + os.environ.get("POSTPORT")
        headers = {'Content-type': 'application/json'}
        r = requests.post(postURL, data=json.dumps({"CCTRANS": transaction}), headers=headers)
        transactionCount+=1
        if transactionCount==numTransactions:
            complete=True


# CREATE TABLE AND WRITE THE CUSTOMERS TO THAT TABLE
# GENERATE TRANSACTION WILL THEN USE THAT AS BASE AND SHOW TRANSACTIONS FOR CUSTOMERS
# PHASE 1 will be generic and random transactions
#        Query Table to get a user and generate a transaction mostly in own territor
# FROM CUSTOMER NUMBER, LAT,LONG,CC#  then add itemdesc,category,amount,timestamp,companyname,lat,long

def cliParse():
    VALID_ACTION = ["all","trans"]
    parser = argparse.ArgumentParser(description='Financial Data Generator')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_all = subparsers.add_parser("all", help="Run Database creation and Gen Transactions")
    parser_transactions = subparsers.add_parser("trans", help="Generate Transactions")


    parser_all.add_argument("--cust", dest='numCustomers', action="store",help="Number of Customers to Create",required=True)
    parser_all.add_argument("--trans", dest='numTransactions', action="store",help="Number of Transactions to Generate",required=True)


    args = parser.parse_args()

    if (args.subparser_name == "all"):
        numCustomers = int(args.numCustomers)
        numTransactions = int(args.numTransactions)
        return numCustomers,numTransactions


    elif (args.type == "trans"):
            print "Not Yet Implemented"



if __name__ == '__main__':

    numCustomers,numTransactions=cliParse()

    dotenv_path = "./config/config.env"
    load_dotenv(dotenv_path)
    logger = logging.getLogger()
    #logger.addHandler(SysLogHandler(address=(os.environ.get("DBHOST"), 1514)))
    logger.setLevel(logging.INFO)

    #numCustomers=40
    customers=[]
    addAddressesRedis()
    r = redis.Redis(host=os.environ.get("GENHOST"), port=6379, db=0)
    r.flushdb()
    customerAges = ageDistribution(numCustomers)
    loadBankingData()
    for x in range(0,numCustomers):
        customer = []
        customer = buildCustomer(x,customerAges[x])
        addCustomerRedis(r,customer[0],customer)
        customers.append(customer)

    outputCustomers(customers)



    loadCustomerTable()
    generateTransactions(numTransactions, numCustomers)
    buildTransactionTables()


    #outputCustomers(customers)
    #getCustomer()




