import argparse
from dotenv import load_dotenv
import os
from faker import Faker
from objects.Customer import Customer
from objects.CustomerCreditData import CustomerCreditData
from objects.Transaction import Transaction
import csv
import numpy as np
import math
from datetime import timedelta,datetime
import pickle
import pprint,json
import queries
import requests
import time

dotenv_path = "./config/config.env"
seedCustomerNumber = 40000


def cliParse():
    VALID_ACTION = ["db","txs","all"]
    parser = argparse.ArgumentParser(description='Financial Data Generator')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_all = subparsers.add_parser("all", help="Create Customers Database and Generate Transactions against it")
    parser_txs = subparsers.add_parser("txs", help="Generate Transactions to Existing Customers")
    parser_db  = subparsers.add_parser("db", help="Create Customers Database")


    parser_db.add_argument("--cust", dest='numCustomers', action="store",help="Number of Customers to Create",required=True)
    parser_db.add_argument("--test", dest='test', action="store_true",help="TESTING FLAG",required=False)


    parser_all.add_argument("--cust", dest='numCustomers', action="store",help="Number of Customers to Create",required=True)
    parser_all.add_argument("--trans", dest='numTransactions', action="store",help="Number of Transactions to Generate",required=True)
    parser_all.add_argument("--test", dest='test', action="store_true",help="TESTING FLAG",required=False)
    parser_all.add_argument("--dbload", dest='dbload', action="store_true",help="Load GPDB/HAWQ",required=False)


    args = parser.parse_args()

    if (args.subparser_name == "db"):
        if args.test:
            os.environ["FDGTEST"] = "True"


        numCustomers = int(args.numCustomers)
        dbLoad = args.dbload

        return numCustomers,0,"db",dbLoad
    elif (args.subparser_name == "txs"):
        print "Not Yet Implemented"
        return 0,0,"txs"
    elif (args.subparser_name == "all"):
        if args.test:
            os.environ["FDGTEST"] = "True"

        numCustomers = int(args.numCustomers)
        numTransactions =  int(args.numTransactions)
        dbLoad = args.dbload
        return numCustomers, numTransactions, "all",dbLoad


def generateTransactions(numTransactions,newCustomers,customers):
    print "Generating Transactions..."
    # Using Given Customer, Generate a Transaction, Pass ot back.
    transactions = []
    fake = Faker()

    # If Existing DB:  Customers Created prior to execution
    # If not, there will already be a set of customers array

        # normal distribution of transaction location




    count = 0
    zipCodeDataset = readZipCodeDataset()


    for x in range(1,numTransactions):
        transaction = Transaction()
        if newCustomers:
            transaction.customerNumber = seedCustomerNumber + np.random.randint(0,numCustomers - 1)  # Pick a random Customer - Even Distribution


        # Make some Remote Transactions
            num =  np.random.normal(0,1)
            if num > 1.6 or num < -1.6:
               transaction.city, transaction.state, transaction.zip, transaction.latitude, transaction.longitude = getAddressData(zipCodeDataset)
            else:
                transaction.city = customers[(transaction.customerNumber - seedCustomerNumber)].city
                transaction.state = customers[(transaction.customerNumber - seedCustomerNumber)].state
                transaction.zip = customers[(transaction.customerNumber - seedCustomerNumber)].zip
                transaction.latitude = customers[(transaction.customerNumber - seedCustomerNumber)].latitude
                transaction.longitude = customers[(transaction.customerNumber - seedCustomerNumber)].longitude

        transaction.streetAddress = str(fake.street_address())
        transaction.transactionTimestamp = (str(datetime.now()))
        transaction.amount = (float(transactionDistribution()))
        transaction.id = time.strftime("%y%m%d%H%m%s")+str(x)
        transaction.flagged = 0
        postTransaction(transaction)
        transactions.append(transaction)
    return transactions

def transactionDistribution():
    transMax = 10001
    transMin = 1
    transMean = 100


    # Tried 100 different ways to get what I wanted, but settled on the easiest.
    # Used a triangular distribution from -4000 - 0 - 10000
    # Then take abs of the negative numbers which basically folds that portion
    # of the triangle ontop of the other positives.  This gives me additional
    # low transaction numbers which helps bring my mean down to a reasonable level.

    values = np.random.triangular(-4000, transMin, transMax, 1)
    return np.round(np.abs(values),2)

def writeCustomerDataset(customers,setName):
    print "Output Customer Dataset"
    # with open("./data/customers.dat", "wb") as customersFile:
    #     pickle.dump(customers,customersFile,2)

    with open("./data/"+setName+".csv", "wb") as customersFile:
        writer = csv.writer(customersFile,quotechar='"',quoting=csv.QUOTE_NONNUMERIC,delimiter=',')

        keys = vars(customers[0]).keys()
        writer.writerow(keys)
        for customer in customers:
            writer.writerow(vars(customer).values())

def writeTransactionDataset(customers,setName):
    print "Output Transaction Dataset"
    # with open("./data/customers.dat", "wb") as customersFile:
    #     pickle.dump(customers,customersFile,2)

    with open("./data/"+setName+".csv", "wb") as customersFile:
        writer = csv.writer(customersFile,quotechar='"',quoting=csv.QUOTE_NONNUMERIC,delimiter=',')

        keys = vars(transactions[0]).keys()
        writer.writerow(keys)
        for transaction in transactions:
            writer.writerow(vars(transaction).values())


def readCustomerCreditDataset():
    print "Read Customer Credit Data CSV"
    customerCreditDataset = []
    with open('./data/german_credit.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            customerCreditData = CustomerCreditData(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20])
            customerCreditDataset.append(customerCreditData)
    return customerCreditDataset

def readZipCodeDataset():
    zipCodeDataset = []
    with open('./data/zipcodes.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            zipCodeData = {}
            zipCodeData["zipCode"] = row[0]
            zipCodeData["city"] = row[2]
            zipCodeData["state"] = row[3]
            zipCodeData["latitude"] = row[5]
            zipCodeData["longitude"] = row[6]
            zipCodeDataset.append(zipCodeData)
    print "ZipCodes Returned"
    return zipCodeDataset


def getAddressData(zipCodeDataSet,zipCode=""):

    # PASSING IN A ZIP CODE WILL NOT WORK.  NOT REALLY A BUG
    if zipCode == "":
        zipCodeIndex = np.random.random_integers(0,len(zipCodeDataSet)-1)
    else:
        print "NOT IMPLEMENTED"
        # WILL HAVE TO SEARCH THE WHOLE LIST SINCE IT"S NOT INDEXED ON ZIPCODE

    zipCode = zipCodeDataSet[zipCodeIndex]["zipCode"]
    city = zipCodeDataSet[zipCodeIndex]["city"]
    state = zipCodeDataSet[zipCodeIndex]["state"]
    latitude = zipCodeDataSet[zipCodeIndex]["latitude"]
    longitude = zipCodeDataSet[zipCodeIndex]["longitude"]
    #print "WATCH FOR ERROR:",latitude,longitude
    return city, state, zipCode, float(latitude), float(longitude)

def getBirthDate(age):
    fake = Faker()
    today = datetime.now()

    birthDate = fake.date_time()

    #Check to see if month day is before todays month day
    if birthDate.month < today.month:
        birthYear =  (today - timedelta(days=int(age)*365.24)).year
    elif ((birthDate.month == today.month) and (birthDate.day <= today.day)):
        birthYear = (today - timedelta(days=int(age)*365.24)).year
    else:
        birthYear = (today - timedelta(days=(int(age)-1)*365.24)).year


    try:
        newDate = birthDate.replace(year=birthYear)
    except ValueError:
        newDate = birthDate.replace(year=birthYear,day=15)

    return newDate


def buildCustomerDB(numCustomers):
    print "Build Customers"
    # Generate Random Customer Data, and Read Banking Data and COmbine into Customer object
    # Seed the Customer Number to make it look "Real"
    fake = Faker()

    # LOAD BANKING DATASET
    customerCreditDataset = readCustomerCreditDataset()

    # LOAD ZIPCODE DATASET
    zipCodeDataset = readZipCodeDataset()
    customers = []

    for i in range(0,numCustomers):
        customerData=Customer()

        # Cycle Back through Data and just repeat if more than 1000 rows are needed.
        if i < 1000:
            customerCreditIndex = i
        else:
            customerCreditIndex = int(i - (math.floor(i/1000)*1000))

        customerData.customerNumber = seedCustomerNumber + i



        if customerCreditDataset[customerCreditIndex].sexMaritalStatus == 0:
            firstName = fake.first_name_male()
            customerData.firstName=firstName
        else:
            firstName = fake.first_name_female()
            customerData.firstName=firstName

        lastName = fake.last_name()
        customerData.lastName = lastName

        customerData.streetAddress=fake.street_address()
        customerData.city, customerData.state, customerData.zip, customerData.latitude, customerData.longitude = getAddressData(zipCodeDataset)
        customerData.cardNumber=fake.credit_card_number()
        customerData.phoneNumber=fake.phone_number()
        customerData.socialsecurityNumber=fake.ssn()
        customerData.birthDate = getBirthDate(customerCreditDataset[customerCreditIndex].age)
        customerData.emailAddress = firstName[0] + lastName + "@" + fake.free_email_domain()
        customerData.job = (fake.job()).translate(None, "'")

        # Generate Truncated Normal Distribution
        # used Manual "truncation", by basically taking absolute value of the negative numbers.
        # USE MEAN of 3000

        #customerData.append(format(np.round(np.abs(np.random.normal(3000,3000)),2), '.2f'))
        customerData.accountBalance = format(np.round(np.abs(np.random.normal(3000,3000)),2), '.2f')

        #
        # ACCOUNTBALANCE
        # THEN AGE AND CALCULATE A BIRTHDATE BASED ON AN AGE.


        customerData.age = int(customerCreditDataset[customerCreditIndex].age)
        customerData.creditability = int(customerCreditDataset[customerCreditIndex].creditability)
        customerData.accountBalanceStatus = int(customerCreditDataset[customerCreditIndex].accountBalanceStatus)
        customerData.creditDuration = int(customerCreditDataset[customerCreditIndex].creditDuration)
        customerData.paymentStatusPrevCredit = int(customerCreditDataset[customerCreditIndex].paymentStatusPrevCredit)
        customerData.purpose = int(customerCreditDataset[customerCreditIndex].purpose)
        customerData.creditAmount = int(customerCreditDataset[customerCreditIndex].creditAmount)
        customerData.savingsValue = int(customerCreditDataset[customerCreditIndex].savingsValue)
        customerData.employmentLength = int(customerCreditDataset[customerCreditIndex].employmentLength)
        customerData.creditPercent = int(customerCreditDataset[customerCreditIndex].creditPercent)
        customerData.sexMaritalStatus = int(customerCreditDataset[customerCreditIndex].sexMaritalStatus)
        customerData.guarantors = int(customerCreditDataset[customerCreditIndex].guarantors)
        customerData.durationAddess = int(customerCreditDataset[customerCreditIndex].durationAddess)
        customerData.mostValAsset = int(customerCreditDataset[customerCreditIndex].mostValAsset)
        customerData.existingLines = int(customerCreditDataset[customerCreditIndex].existingLines)
        customerData.typeResidence = int(customerCreditDataset[customerCreditIndex].typeResidence)
        customerData.existingLinesBank = int(customerCreditDataset[customerCreditIndex].existingLinesBank)
        customerData.employmentType = int(customerCreditDataset[customerCreditIndex].employmentType)
        customerData.dependents = int(customerCreditDataset[customerCreditIndex].dependents)
        customerData.telephoneAvail = int(customerCreditDataset[customerCreditIndex].telephoneAvail)
        customerData.foreignWorker = int(customerCreditDataset[customerCreditIndex].foreignWorker)
        customers.append(customerData)



    return customers


def postTransaction(transaction):

    #jsonTransaction = json.dumps({"CCTRANS" :vars(transaction)})
    jsonTransaction = json.dumps(vars(transaction))
    postURL = "http://" + os.environ.get("POSTSERVER") + ":" + os.environ.get("POSTPORT")
    headers = {'Content-type': 'application/json'}
    r = requests.post(postURL, data=jsonTransaction, headers=headers)



def loadDatabase():
    dbURI = queries.uri(os.environ.get("DBHOST"), port=os.environ.get("DBPORT"), dbname="gpadmin", user="gpadmin",
                        password="gpadmin")
    with queries.Session(dbURI) as session:
        result = session.query("drop table if exists customers CASCADE ;")
        result = session.query("create table customers(existingLines int,birthDate date,creditAmount int,guarantors int,creditDuration int,cardNumber text,existingLinesBank int,city text,typeResidence int,zip text,employmentType int,mostValAsset int,streetAddress text,state text,creditPercent int,phoneNumber text,latitude float,employmentLength int,accountBalanceStatus int,job text ,paymentStatusPrevCredit int,emailAddress text,purpose int,foreignWorker int,sexMaritalStatus int,creditability int,firstName text,accountBalance float,lastName text,age int,longitude float,savingsValue int,socialsecurityNumber text,dependents int,customerNumber bigint,durationAddess int,telephoneAvail int) with (appendonly=true) DISTRIBUTED RANDOMLY;")

        with open('./data/customers.csv') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            for row in reader:
                rowString = "'" + "','".join(row) + "'"
                result = session.query("insert into customers VALUES (" + rowString + ");")

                # This is to post new customers.   Not implementing yet.
        result = session.query("drop table if exists transactions CASCADE ;")
        result = session.query("drop table if exists transactions_hive CASCADE ;")

        result = session.query("create table transactions(city text,zip integer,amount float,flagged int,state text,longitude float,id text,streetaddress text,latitude float,transactiontimestamp timestamp,customerNumber bigint) with (appendonly=true) DISTRIBUTED RANDOMLY;")
        result = session.query("create table transactions_hive(like transactions);")

        result = session.query("drop external table if exists transactions_pxf CASCADE ;")
        result = session.query("create external table transactions_pxf(like transactions) LOCATION('pxf://" + os.environ.get("DBHOST") + ":51200/scdf/*.txt?PROFILE=HDFSTextSimple') FORMAT 'CSV' (QUOTE '''')  LOG ERRORS INTO err_transactions SEGMENT REJECT LIMIT 500;")

                #postURL = "http://" + os.environ.get("POSTSERVER") + ":" + str(int(os.environ.get("POSTPORT")) + 1)
                #headers = {'Content-type': 'application/json'}
                ## rowJSON = {"customerNum": row[0]},{"accountBalanceStatus":row[16]},{"paymentStatusPrevCredit":row[18]},{"typeResidence":row[29]},{"existingLines":row[28]},{"existingLinesBank":row[30]},{"creditPercent": row[23]},{"creditDuration":row[17]},{"creditAmount":row[20]}
                #rowJSON = {"customerNum": row[0], "accountBalanceStatus": row[16], "paymentStatusPrevCredit": row[18],
                #           "typeResidence": row[29], "existingLines": row[28], "existingLinesBank": row[30],
                #           "creditPercent": row[23], "creditDuration": row[17], "creditAmount": row[20]}
                #print rowJSON
                #r = requests.post(postURL, data=json.dumps(rowJSON), headers=headers)
                # print r


def loadTrainingSets():
    print "LOADING TRAINING DATA SETS"
    dbURI = queries.uri(os.environ.get("DBHOST"), port=os.environ.get("DBPORT"), dbname="gpadmin", user="gpadmin",
                        password="gpadmin")
    with queries.Session(dbURI) as session:
        result = session.query("drop table if exists customers_train CASCADE ;")
        result = session.query("create table customers_train(existingLines int,birthDate date,creditAmount int,guarantors int,creditDuration int,cardNumber text,existingLinesBank int,city text,typeResidence int,zip text,employmentType int,mostValAsset int,streetAddress text,state text,creditPercent int,phoneNumber text,latitude float,employmentLength int,accountBalanceStatus int,job text ,paymentStatusPrevCredit int,emailAddress text,purpose int,foreignWorker int,sexMaritalStatus int,creditability int,firstName text,accountBalance float,lastName text,age int,longitude float,savingsValue int,socialsecurityNumber text,dependents int,customerNumber bigint,durationAddess int,telephoneAvail int) with (appendonly=true) DISTRIBUTED RANDOMLY;")

        with open('./data/customers-train.csv') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            for row in reader:
                rowString = "'" + "','".join(row) + "'"
                result = session.query("insert into customers_train VALUES (" + rowString + ");")

    result = session.query("drop table if exists customers_train CASCADE ;")
    result = session.query(
        "create table customers_train(existingLines int,birthDate date,creditAmount int,guarantors int,creditDuration int,cardNumber text,existingLinesBank int,city text,typeResidence int,zip text,employmentType int,mostValAsset int,streetAddress text,state text,creditPercent int,phoneNumber text,latitude float,employmentLength int,accountBalanceStatus int,job text ,paymentStatusPrevCredit int,emailAddress text,purpose int,foreignWorker int,sexMaritalStatus int,creditability int,firstName text,accountBalance float,lastName text,age int,longitude float,savingsValue int,socialsecurityNumber text,dependents int,customerNumber bigint,durationAddess int,telephoneAvail int) with (appendonly=true) DISTRIBUTED RANDOMLY;")

    with open('./data/transactions-train.csv') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        for row in reader:
            rowString = "'" + "','".join(row) + "'"
            result = session.query("insert into transactions_train VALUES (" + rowString + ");")


if __name__ == '__main__':
    load_dotenv(dotenv_path)
    numCustomers,numTransactions,action,dbLoad=cliParse()
    if (os.getenv("FDGTEST")):
        print "Running Tests"
        customers = buildCustomerDB(numCustomers)
        transactions = generateTransactions(100,True,customers)
        writeCustomerDataset(customers)
        writeTransactionDataset(transactions)


        #readZipCodeDataset()
        exit()
    if "db" in action:
        customers = buildCustomerDB(numCustomers)
        writeCustomerDataset(customers)
    elif "all" in action:
        print dbLoad

        print "Generating Training Set"
        customers = buildCustomerDB(numCustomers)
        writeCustomerDataset(customers,"customers-training")
        transactions = generateTransactions(int(np.round(numTransactions*.1,0)),True,customers)


        writeTransactionDataset(transactions,"transactions-training")
        print "Generating Real Data"
        customers = buildCustomerDB(numCustomers)
        writeCustomerDataset(customers,"customers")
        transactions = generateTransactions(numTransactions, True, customers)
        writeTransactionDataset(transactions,"transactions")
        if dbLoad:
            loadDatabase()

    print numCustomers,numTransactions,action



