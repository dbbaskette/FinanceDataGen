class Transaction(object):
    """A customer of with a checking account. Customers have the
    following properties:

    Attributes:
        name: A string representing the customer's name.
        balance: A float tracking the current balance of the customer's account.
    """




    def __init__(self):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.customerNumber = 0
        self.streetAddress = ""
        self.city=""
        self.state = ""
        self.zip = ""
        self.latitude = 0
        self.longitude = 0
        self.transactionTimestamp =  0
        self.amount = 0


