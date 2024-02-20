import re
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urldefrag
import os
import sys
import csv
from data import Data
import psutil #to detect memory usage -- need to install using pip install psutil
import time #to calculate time it takes to retrieve the query
import math

WORDFREQ = {} #global words frequencies
NUM_WORDS = 0 #total number of words
PAGE_INDEXES = {} #{int, URL}
INVERTED_INDEX = {} #{word, recording page indx, frequency in page, position(may add later)} //Start of simple inverted index
INDEX_OF_INDEX = {} #{pageIndex, position in file}
P_INDEX = 0 #current page index
DATA_STREAMS = {} #dictionary holding streams for all open data files
INDEX_STREAMS = {} #dictionary holding streams for all open index files

#Tracking dumped files -- numbers for index of index correspond with file numbers
fileNum = 0
dumpSize = 5000000 #5000000 optimal for 5 indexes

numDocsProccessed = 0

#Reading/Saving/Dumping Methods for Index Storage
def readDataFile(fileName):
  #constructs into relevant dictionary and returns it (for testing only)
  fileDict = {}
  #Credit for encoding fix: https://www.reddit.com/r/learnpython/comments/108bo9y/charmap_codec_cant_encode_character_x_character/
  f = open(fileName, "r", encoding='utf-8')
  for line in f:
    #Strip newline
    line.strip()
    parsedLine = line.split(" ")
    key = parsedLine[0]
    fileDict[key] = {}
    #Starting from index 1, parse data relevant to term
    for items in parsedLine[1:len(parsedLine)-1]:
      parsedItem = items.split(",")
      pageIndex = parsedItem[0]
      frequency = parsedItem[1]
      print(parsedItem)
      fileDict[key][pageIndex] = Data(frequency) #data object creation here
  #f.close()
  return fileDict

def readDataLine(fileStream, position):
  #gets specified line from file name using indx of indx (must be intiialized already)
  fileDict = {}
  #Credit for encoding fix: https://www.reddit.com/r/learnpython/comments/108bo9y/charmap_codec_cant_encode_character_x_character/
#   f = open(fileName, "r", encoding='utf-8')
  #Utilize index of index to get line and process it
  fileStream.seek(position)
  line = ""
  try:
    line = fileStream.readline()
  except:
      print("Error in reading term")
  #Process line
  if(line != ""):
    strippedLine = line.strip() #remove newline
    parsedLine = strippedLine.split(" ")
    key = parsedLine[0]
    #print(parsedLine)
    fileDict[key] = {}
    #Parse all items related to the line
    for items in parsedLine[1:len(parsedLine)]:
        parsedItem = items.split(",")
        pageIndex = parsedItem[0]
        frequency = parsedItem[1]
        #print(parsedItem)
        fileDict[key][pageIndex] = Data(frequency)
    #f.close
    #Return pointer back to beginning?
    fileStream.seek(0)
  return fileDict

def readIndexFile(fileStream, currFileNum, token):
  #returns full index of index file for specified file
  #Credit for encoding fix: https://www.reddit.com/r/learnpython/comments/108bo9y/charmap_codec_cant_encode_character_x_character/
  #f = open(fileName, "r", encoding='utf-8')
  alphaFileName = "indexes/alphaIndex" + str(currFileNum) + ".txt"
  alphaPositions = {}
  #Load in for alphaDict
  alphaFile = open(alphaFileName, 'r', encoding = 'utf-8')
  #Fill alpha dictionary
  rawData = alphaFile.readline()
  cleanData = rawData.strip(" ")
  parsedLine = cleanData.split(" ")
  i = 0
  while i in range(len(parsedLine)):
    key = parsedLine[i]
    i += 1
    position = parsedLine[i]
    i += 1
    alphaPositions[key] = int(position)
  #Look at token's first letter and start get position for index of index
  if token[0] in alphaPositions:
    startPosition = alphaPositions[token[0]]
    fileStream.seek(startPosition) #manipulates fileStream to search from a certain starting point
    for line in fileStream:
        #Strip newline
        if(token[0] == line[0]):
            line.strip("\n")
            #Parse into correct dictionary
            parsedLine = line.split(" ")
            key = parsedLine[0]
            value = parsedLine[1]
            INDEX_OF_INDEX[key] = int(value)
        elif(token[0] < line[0]):
            break #break look if larger
  alphaFile.close() #close file after reading

def dumpIndexofIndex(fileNum):
  #Concurrent creation of alphaIndex
  filePos = 0
  alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
  alphaFileName = "indexes/alphaIndex" + str(fileNum) +".txt"
  alphaDict = {'a':0}
  currLetterIndex = 1
  #Properly formats index of index
  fileName = "indexes/index" + str(fileNum) +".txt"
  #Credit for encoding fix: https://www.reddit.com/r/learnpython/comments/108bo9y/charmap_codec_cant_encode_character_x_character/
  f = open(fileName, "w", encoding='utf-8')  #write, because we are only writing once per dump
  for key, value in INDEX_OF_INDEX.items():
    #record first word of each letter
    if(currLetterIndex < len(alphabet) and key[0] in alphabet and key[0] == alphabet[currLetterIndex]):
        #First instance of key word - might break on foreign chars
        currLetter = alphabet[currLetterIndex]
        alphaDict[currLetter] = filePos
        currLetterIndex += 1
    #process datastring
    dataString  = key + " " + str(value) + "\n"
    filePos += len(dataString) + 1 #accounting for newline
    f.write(dataString)
  f.close()
  #Write alphaIndex to relevant file
  f = open(alphaFileName, 'w', encoding='utf-8')
  for key, value in alphaDict.items():
      dataString = key + " " + str(value) + " "
      f.write(dataString)
  f.close()
  INDEX_OF_INDEX.clear()

def dumpToDisk(fileNum):
  filePos = 0
  #Write current dictionary to file
  #Credit for encoding fix: https://www.reddit.com/r/learnpython/comments/108bo9y/charmap_codec_cant_encode_character_x_character/
  fileName = "indexes/data" + str(fileNum) +".txt"
  sortedIndex = dict(sorted(INVERTED_INDEX.items(), key=lambda item: item[0], reverse=False))
  f = open(fileName, "a", encoding='utf-8')
  for i, j in sortedIndex.items():
    dataString = ""
    dataString += i + " "
    for key, value in j.items():
      dataString += str(key) + "," + str(value.getData()['frequency']) + " "
    dataString += '\n'
    INDEX_OF_INDEX[i] = filePos
    filePos += len(dataString)+1 #accounting for '\n'
    f.write(dataString)
  f.close()
  #Write out index of index
  dumpIndexofIndex(fileNum)
  INVERTED_INDEX.clear() #clear dictionary inside
  return fileNum + 1 #modify fileNumber to keep indexes/data organized

def clearIndexFolder():
    #Utility to clear index folder of old saved indexes
    #Source for deleting files in a folder: https://linuxize.com/post/python-delete-files-and-directories/
    for i in range(fileNum):
        dataPathName = "indexes/data"+str(i)+".txt"
        indexPathName = "indexes/index"+str(i)+".txt"
        if os.path.exists(dataPathName):
            print("Removing " + dataPathName)
            os.remove(dataPathName)
        if os.path.exists(indexPathName):
            print("Removing " + indexPathName)
            os.remove(indexPathName)
#End of Reading/Saving/Dumping Methods for Index Storage

#File Utility Methods
def openFiles(fileNum):
    #open all files before reading them
    for i in range(fileNum):
        dataPathName = "indexes/data"+str(i)+".txt"
        indexPathName = "indexes/index"+str(i)+".txt"
        #Citation for encoding error: https://stackoverflow.com/questions/12468179/unicodedecodeerror-utf8-codec-cant-decode-byte-0x9c
        data = open(dataPathName, "r", encoding='utf-8', errors='ignore')
        index = open(indexPathName, "r", encoding='utf-8', errors='ignore')
        #Create key
        DATA_STREAMS[i] = data
        INDEX_STREAMS[i] = index

def closeFiles(fileNum):
    #close all files before closing program
    for i in range(fileNum):
        DATA_STREAMS[i].close() #close data stream
        INDEX_STREAMS[i].close() #close index stream
#End of File Utility Methods

#Cache Methods for Development
def saveCache():
    #saves number of files to a singular file
    f = open("cacheInfo.txt", "w", encoding='utf-8')
    f.write(str(fileNum))
    f.write("\n" + str(numDocsProccessed))
    f.close()
    f = open("pageIndexCache.txt", "w", encoding='utf-8')
    for key, value in PAGE_INDEXES.items():
        f.write(str(key) + " ")
        f.write(value + " ")

def loadCache():
    #Sets fileNum
    try:
        f = open("cacheInfo.txt", "r", encoding='utf-8')
        info = f.readline()
        numDocs = f.readline()
        return (int(info), int(numDocs)) #holds fileNumber
    except:
        #If file does not exist, set fileNum to 0
        return (0, 0)

def loadPageIndexCache():
        #Load Page Indexes
        f = open("pageIndexCache.txt", "r", encoding='utf-8')
        rawData = f.readline()
        f.close()
        #Process Data and Put into Index
        strippedData = rawData.strip()
        strippedData = strippedData.split(" ")
        #print("Length of stripped data:",len(strippedData))
        i = 0
        while i in range(len(strippedData)):
            pageIndex = int(strippedData[i])
            i+= 1
            url = strippedData[i]
            i+=1
            PAGE_INDEXES[pageIndex] = url
#End of Cache Methods

#Pass in parent file path into this function to process Documents
def processDocs(filePath):
    global P_INDEX, fileNum, numDocsProccessed #use the global P_INDEX due to assignment
    dummyCounter = 0

    subPath = getSubFoldersPath(filePath)

    for subFile in subPath:
        # dummyCounter += 1
        # if dummyCounter == 5:
        #     break
        for docs in getDocsPath(subFile):
            numDocsProccessed += 1
            jsonFile = open(docs)
            jsonData = json.load(jsonFile)

            #getting the content from the parsed json files
            soup = BeautifulSoup(jsonData["content"], "lxml")
            url = BeautifulSoup(jsonData['url'], "lxml") #retrieve url for indexing
            url_txt = url.getText()
            if(url_txt not in PAGE_INDEXES.values()):
                PAGE_INDEXES[P_INDEX] = url_txt
                P_INDEX = P_INDEX + 1
            bodyContent = ""
            for body in soup.find_all('body'):
                bodyContent += body.getText()

            #tokenizing the json data
            tokenlist = tokenize(bodyContent)
            #removed special characters and numbers
            cleaned_list = [word.lower() for word in tokenlist if word.isalpha()]

            for word in cleaned_list:
                if word in WORDFREQ:
                    WORDFREQ[word] += 1
                else:
                    WORDFREQ[word] = 1
                #Simple Inverted Index Processing
                if word in INVERTED_INDEX:
                    #Check if the page index exists in the list -- Accounts for frequency
                    if(P_INDEX in INVERTED_INDEX[word]):
                        INVERTED_INDEX[word][P_INDEX].frequency += 1
                    else:
                        INVERTED_INDEX[word][P_INDEX] = Data(1)
                else:

                    INVERTED_INDEX[word] = {}
                    INVERTED_INDEX[word][P_INDEX] = Data(1)
            #Check if words over for each doc - Currently Aribitrary Number for Testing
            if(sys.getsizeof(INVERTED_INDEX) > dumpSize):
                fileNum = dumpToDisk(fileNum) #updates fileNum

    #print("Number of Docs Processed: " + str(numDocsProccessed))
    #print(PAGE_INDEXES)
    #print("Number of Unique words: " + str(len(WORDFREQ)))
    # used this to find out how big the inverted index is
    # https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
    #print("Size of Inverted Index in bytes: " + str(sys.getsizeof(INVERTED_INDEX)))
    return True

def getSubFoldersPath(parentFile):
    subFoldersPath = []
    subFolders = os.listdir(parentFile)

    #getting the subfolder path to parse data within them
    for sub in subFolders:
        subFoldersPath.append( (parentFile) + "/" + sub)

    return subFoldersPath

def getDocsPath(filePath):
    docs = os.listdir(filePath)
    docsPath = []
    for doc in docs:
        docsPath.append( (filePath) + "/" + doc)
    return docsPath


#referenced https://stackoverflow.com/questions/6202549/word-tokenization-using-python-regular-expressions
def tokenize(content):
    #returns a list of the tokenized content
    return  re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+", content)


def singleTokenQwery(token):
    class valueData:
        # give each url a logarithmic weight
        urlWeightDict = {} #url : log term frequency weight
        def __init__(self, url, freq):
            freq = int(freq)
            weight = 1+math.log10(freq)
            self.urlWeightDict[url] = weight
        def getTfIdfWeightforURL(self, url):
            tf = self.urlWeightDict[url]
            documentFreq = len(self.urlWeightDict)
            idf = math.log10(numDocsProccessed/documentFreq)
            return tf*idf

    token = token.lower()
    print("Tokenized Query : " + token)
    #Begin timer
    start = time.time()
    #Open all files
    openFiles(fileNum)
    tempDict = {}
    for i in range(fileNum):
        # dataPathName = "indexes/data"+str(i)+".txt"
        # indexPathName = "indexes/index"+str(i)+".txt"
        readIndexFile(INDEX_STREAMS[i], i, token) #retrieve relevant index file
        if(token in INDEX_OF_INDEX.keys()):
            result = readDataLine(DATA_STREAMS[i], INDEX_OF_INDEX[token])

            for results  in result.values():
                for key, value in results.items(): #key = pageIndex, value = Data object
                        tempDict[token] = valueData(key, value.frequency)
        INDEX_OF_INDEX.clear() #clear index after to ensure accuracy between each file
    #End of retrieving query
    sortedWordFreq = sorted(tempDict[token].urlWeightDict.items(), key=lambda item: tempDict[token].getTfIdfWeightforURL(item[0]), reverse=True)

    tokenDictKeys = list(sortedWordFreq)
    pagekeys = [pair[0] for pair in tokenDictKeys]

    for num in range(5):
        if num in range(len(pagekeys)):
            print(PAGE_INDEXES[int(pagekeys[num])])
    end = time.time()
    print("Query Timer Results:", (end-start)*1000, "ms")
    #Close to refresh files
    closeFiles(fileNum)

    # tokenDict = dict(sorted(INVERTED_INDEX[token].items(), key=lambda item: item[1].frequency, reverse=True))
    # # sort token dict by most popular
    # tokenDictKeys = list(tokenDict.keys())
    # print("Top 5 sites found:")
    # for num in range(5):
    #     if num in range(len(tokenDictKeys)):
    #         print(PAGE_INDEXES[tokenDictKeys[num]])

def multiTokenQuery(tokens):
    class valueData:
        # give each url a logarithmic weight
        urlWeightDict = {} #url : log term frequency weight
        def __init__(self, url, freq):
            freq = int(freq)
            weight = 1+math.log10(freq)
            self.urlWeightDict[url] = weight
        def getTfIdfWeightforURL(self, url):
            tf = self.urlWeightDict[url]
            documentFreq = len(self.urlWeightDict)
            idf = math.log10(55379/documentFreq)
            return tf*idf
            

    user_freq_dict = {} #key: token string - value: {list(urls)}

    #Begin timer
    start = time.time()

    openFiles(fileNum)

    # Check if all the terms exist in the Inverted Index
    for token in tokens:
        if token not in user_freq_dict.keys():
            tokenExistsBool = False
            token = token.lower()

            user_freq_dict[token] = None

            for i in range(fileNum):
                readIndexFile(INDEX_STREAMS[i], i, token) #retrieve relevant index file
                if(token in INDEX_OF_INDEX.keys()):
                    tokenExistsBool = True
                    result = readDataLine(DATA_STREAMS[i], INDEX_OF_INDEX[token])

                    for results  in result.values():
                        for key, value in results.items(): #key = pageIndex, value = Data object
                            user_freq_dict[token] = valueData(key, value.frequency)
                INDEX_OF_INDEX.clear() #clear index after to ensure accuracy between each file

            if tokenExistsBool == False:
                print("This Phrase does not exist")
                return
    print("The whole phrase does exist")

    Matching_Indices = []
    # List of the tokens sorted by how long their URL lists are smallest to greatest
    tokensSorted = list(dict(sorted(user_freq_dict.items(), key=lambda item: len(item[1].urlWeightDict))).keys())

    # Compare every term with the term after for like a ((t1 + t2) +t3) effect
    for index in range(len(tokensSorted)):
        if index == len(tokensSorted)-1:
            break
        temp_Indices = []
        # if matching indices is empty, use most frequent token dict
        if Matching_Indices == []:
            current = list(dict(sorted(user_freq_dict[tokensSorted[index]].urlWeightDict.items(), key=lambda item: item[1])).keys())

        
            # list(INVERTED_INDEX[tokensSorted[index]].keys())
        # else just use the matching indices
        else:
            current = Matching_Indices
        next = list(dict(sorted(user_freq_dict[tokensSorted[index+1]].urlWeightDict.items(), key=lambda item: item[1])).keys())
        #next = list(INVERTED_INDEX[tokensSorted[index+1]].keys())
        current_index = 0
        next_index = 0

        # loop through to get all the current matching indices
        while(current_index < len(current) and next_index < len(next)):
            currentDoc = current[current_index]
            nextDoc = next[next_index]
            if int(currentDoc) == int(nextDoc):
                temp_Indices.append(currentDoc)
                current_index += 1
                next_index += 1
            elif currentDoc < nextDoc:
                current_index += 1
            else:
                next_index += 1
        Matching_Indices = temp_Indices

    #End of retrieving query
    
    #Factor in the weights here

    indexDict = {} # Dictionary to log combined weights of all terms for each url
    for term in tokensSorted:
        for url in user_freq_dict[term].urlWeightDict.keys():
            if url not in indexDict:
                indexDict[url] = 0
            indexDict[url] += user_freq_dict[term].getTfIdfWeightforURL(url)
    
    sortedURLs = list(dict(sorted(indexDict.items(), key=lambda item: item[1], reverse=True)).keys())

    # print top 5 matches
    for num in range(5):
        if num in range(len(sortedURLs)): #added for debugging purposes
            print(PAGE_INDEXES[int(sortedURLs[num])])
    end = time.time()
    print("Query Timer Results:", (end-start)*1000, "ms")

if __name__ == '__main__':

    """ print("Processing ANALYST now:")
    if os.path.isfile("analyst.csv"):
        # source for csv loading
        # https://stackoverflow.com/questions/2387697/best-way-to-convert-csv-data-to-dict
        with open('analyst.csv', 'r', encoding='utf-8') as f:
            header = f.readline().strip().split(',')
            dreader = csv.DictReader(f, header)
            INVERTED_INDEX = list(dreader)[0]
    else:
        # source for csv writing
        # https://stackoverflow.com/questions/10373247/how-do-i-write-a-python-dictionary-to-a-csv-file
        test2 = processDocs("ANALYST")
        with open('analyst.csv', 'w', encoding='utf-8') as f:
            w = csv.DictWriter(f, INVERTED_INDEX.keys())
            w.writeheader()
            w.writerow(INVERTED_INDEX)
    print(INVERTED_INDEX) """


    # Reset all variables
    # WORDFREQ = {}  # global words frequencies
    # NUM_WORDS = 0  # total number of words
    # PAGE_INDEXES = {}  # {int, URL}
    # INVERTED_INDEX = {}  # {word, List of Pages word appears in} //Start of simple inverted index
    # P_INDEX = 0  # current page index

    #Cache Begins Here
    (fileNum, numDocsProccessed) = loadCache()
    print("Current number of saved files:",fileNum)
    #If cache does not exist, process documents
    if fileNum == 0:
        #Begin timer
        start = time.time()
        print("\nProcessing DEV now:")
        test2 = processDocs("DEV")
        end = time.time()
        print("Index Parse Timer Results:", (end-start)*1000, "ms")
    else:
        loadPageIndexCache() #load existing page index cache
        #print(PAGE_INDEXES)
    #Testing Read Functionality -- Example for How to Read Through for Specific Terms
    # for i in range(fileNum):
    #     dataPathName = "indexes/data"+str(i)+".txt"
    #     indexPathName = "indexes/index"+str(i)+".txt"
    #     readIndexFile(indexPathName)
    #     if('future' in INDEX_OF_INDEX.keys()):
    #
    #         result = readDataLine(dataPathName, INDEX_OF_INDEX['future'])
    #         print(result)
    #         for resultKey, resultValue in result.items():
    #             for key,value in resultValue.items(): #key = pageIndex, value = Data object
    #                 print(i, value.frequency)
    #     INDEX_OF_INDEX.clear() #clear index after to ensure accuracy between each file

    #Testing Disk Usage Check --
    # disk_info = psutil.disk_usage("/")
    # used = sys.getsizeof(INVERTED_INDEX)
    # print("Inverted Index Disk Usage:", used/1024/1024/1024, "GB")
    # total = disk_info.total #Cite website we got from it (/1024/1024/1024 to get GB)
    # usage_percent = used/total
    # print("Percentage of Disk Usage:", (used/total)/1024/1024/1024, "GB")
    # if(usage_percent >= .5):
    #     print("The index is using half of the total disk memory! Time to dump it!")
    #
    user_input = ""
    while user_input != "Q":
        print("Enter Search Query or Enter Q to quit")
        user_input = input("Enter something: ")
        user_input = user_input.strip()
        if(user_input != ""):
            print("You entered:", user_input)
            user_tokens = tokenize(user_input)
            if len(user_tokens) == 1:
                    singleTokenQwery(user_tokens[0])
            else:
                multiTokenQuery(user_tokens)

#Clear cache and close after closing program
saveCache()
#clearIndexFolder() #Temporarily suspended for fast development on query optimization








    
