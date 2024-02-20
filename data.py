class Data:
    #Class for use recording data per page and scalability
    def __init__(self, frequency):
        self.frequency = frequency #frequency per page
    
    def getData(self):
        #Retrieving relevant data from object to be written to file later --> Maybe returns a values if anything is added to this
        valueDictionary = {'frequency': self.frequency} #values transformed into dictionary for processing
        return valueDictionary