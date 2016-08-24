class Customer(object):
    """A customer of ABC Bank with a checking account. Customers have the
    following properties:

    Attributes:
        name: A string representing the customer's name.
        balance: A float tracking the current balance of the customer's account.
    """

    def __init__(self, firstName, lastName):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.firstName = firstName
        self.lastName = lastName#

