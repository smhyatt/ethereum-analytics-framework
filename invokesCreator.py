import numpy as np
import os, json
import h5py
import time
import gzip
import re
import sys

contractDict, tokenContractDict = ({} for i in range(2)) 


def getRecipient(inputData):
    spender   = None
    nonce     = None
    isEOA     = None
    value     = None
    recipient = None

    if inputData[2:10] == 'a9059cbb':       # transfer event
        recipient = '0x'+inputData[34:74]   # = to address for token holder
        vdata = inputData[75:138]           # = amount
        if re.search(r'\d', vdata):
            value = int(vdata, 16)

        isEOA = not isContract(recipient)


    elif inputData[2:10] == '23b872dd':     # transferFrom event
        inputData[34:74]                    # = token contract address
        vdata = inputData[75:138]           # = amount (hex string)
        if re.search(r'\d', vdata):
            value = int(vdata, 16)
        recipient = '0x'+inputData[162:202] # = token user address

        isEOA = not isContract(recipient)


    elif inputData[2:10] == '095ea7b3':     # approve event
        spender = inputData[34:74]          # = spender
        vdata   = inputData[75:138]         # = amount
        if re.search(r'\d', vdata):
            value = int(vdata, 16)
    else:
        recipient = None

    return (recipient, spender, str(value), isEOA)



def sortTransactions(transactionsLst): 
    idx = 0
    res = []
    for transaction in transactionsLst:

        fromAdr, toAdr, inpt, timest, nonce, fstVal = [transaction[i] for i in range(len(transaction))]

        (recipient, spender, sndVal, endReceiverIsEOA) = getRecipient(inpt)

        if isContract(fromAdr):
            fromCon = True  
        else:
            fromCon = False 


        if isContract(toAdr):
            toCon = True 
        else:
            toCon = False

        if recipient is not None:
            callsdata = np.array((str(fromAdr), str(toAdr), str(recipient), str(spender), str(fromCon), str(toCon), str(endReceiverIsEOA), str(timest),
            str(nonce), str(fstVal), str(sndVal)), dtype=h5py.special_dtype(vlen=str))
            res.append((callsdata, True))
        else:
            transdata = np.array((fromAdr, toAdr, fromCon, toCon, timest, nonce, fstVal), dtype=h5py.special_dtype(vlen=str))
            res.append((transdata, False))

    return res


def sortTokens(tokensLst):
    idx = 0
    res = []
    for token in tokensLst:

        fromAdr, toAdr, inpt, timest, nonce, fstVal = [token[i] for i in range(len(token))]

        (recipient, spender, sndVal, endReceiverIsEOA) = getRecipient(inpt)

        if isContract(fromAdr):
            fromCon = True      
        else:
            fromCon = False     

        if isContract(toAdr):
            toCon = True        
        else:
            toCon = False       

        tokendata = np.array((str(fromAdr), str(toAdr), str(recipient), str(spender), str(fromCon), str(toCon), str(endReceiverIsEOA), str(timest),
        str(nonce), str(fstVal), str(sndVal)), dtype=h5py.special_dtype(vlen=str))
        res.append((tokendata, True))

    return res



def addTokens(dsetTok, tokens):
    tokLen = len(tokens)

    dsetTok.resize(dsetTok.shape[0]+tokLen, axis=0)

    idx1 = -tokLen
    for token in tokens:
        dsetTok[idx1, :] = token
        idx1 += 1


def addTransactions(dsetTran, transactions):
    traLen = len(transactions)
    dsetTran.resize(dsetTran.shape[0]+traLen, axis=0)

    idx2 = -traLen
    for transaction in transactions:
        dsetTran[idx2, :] = transaction
        idx2 += 1


def addConsToCons(dsetCon2C, transactions):
    traLen = len(transactions)
    dsetCon2C.resize(dsetCon2C.shape[0]+traLen, axis=0)

    idx2 = -traLen
    for transaction in transactions:
        dsetCon2C[idx2, :] = transaction
        idx2 += 1


def isContract(addr):
    try:
        contractDict[addr]
        return True
    except Exception as e:
        return False

def isToken(addr):
    try:
        tokenContractDict[addr]
        return True
    except Exception as e:
        return False


def sortData(path, jsonfile):
    openFile = os.path.join(path, jsonfile)
    allList, transactionList, tokenList, tmpList = ([] for i in range(4)) 

    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    for di in allList:
        if 'to_address' in di:
            if isToken(di['to_address']):       # a token
                tmpList = [di['from_address'],di['to_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
                tokenList.append(tmpList)

            else:                               # regular transaction
                tmpList = [di['from_address'],di['to_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
                transactionList.append(tmpList)

    return (tokenList, transactionList)



def fileExecutor(tokenDset, transDset, con2CDset, path, jsonlist):
    for jsonfile in jsonlist:
        fileStartTime = time.time()
        (tokenList, transactionList) = sortData(path, jsonfile)
        
        tokens, cons2cons, transactions = ([] for i in range(3)) 

        tranRes = sortTransactions(transactionList)
        tokens  = sortTokens(tokenList)

        print(tokens)
        print(tokens.shape())


        for el in tranRes:
            elem = el[0]
            isCall = el[1]

            if isCall:
                cons2cons.append(elem)
            else:
                transactions.append(elem)


        addTokens(tokenDset, tokens)
        addTransactions(transDset, transactions)
        addConsToCons(con2CDset, cons2cons)


        fileEndTime  = time.time() - fileStartTime
        filesVisited = open("database/transactions/seenfiles.txt", "a+")
        timeSpan     = open("database/transactions/timespan.txt", "a+")

        filesVisited.write(str(jsonfile)+'\n')
        timeSpan.write(str(fileEndTime)+", "+str(len(transactionList))+", "+str(len(tokenList))+'\n')
        print("File added", jsonfile)

        filesVisited.close()
        timeSpan.close()





def main(arg):
    val = int(arg[1])
    global contractDict
    global tokenContractDict

    jsonlist, contractLst, tokenLst = ([] for i in range(3)) 

    f1 = open('contractlist.txt', 'r')
    for con in f1:
        newcon = con[:-1]
        contractLst.append(newcon)

    contractDict = dict.fromkeys(contractLst)


    f2 = open('tokencontractlist.txt', 'r')
    for tok in f2:
        newtok = tok[:-1]
        tokenLst.append(newtok)

    tokenContractDict = dict.fromkeys(tokenLst)


    path = "/home/crj405/sarah/ethereumchain"
    filelist = os.listdir(path)
    translist = []
    for file in filelist:
        filetype = (file.split("-"))
        if filetype[1] == 'transactions':
            translist.append(file)

    idx = 0
    translist.sort()

    tokenFilename = 'tokendata'+ str(val) +'.hdf5'
    transFilename = 'transdata'+ str(val) +'.hdf5'
    callsFilename = 'callsdata'+ str(val) +'.hdf5'

    tokenDB = h5py.File('database/transactions/'+tokenFilename, 'a')
    transDB = h5py.File('database/transactions/'+transFilename, 'a')
    con2CDB = h5py.File('database/transactions/'+callsFilename, 'a')

    tokenDset = tokenDB['tokens']
    transDset = transDB['transactions']
    con2CDset = con2CDB['contractTransfers']

    for tf in translist:
        file = str(tf)
        
        if idx < val*88:
            idx += 1
            continue

        if file+'\n' in open("database/transactions/seenfiles.txt").read():   # check if we have already read this file before
            print("Skipped file:", file)
            continue                                    # if so, continue to the next file

        if idx-(val*88) == 88:
            break

        jsonlist.append(file)
        idx += 1

    fileExecutor(tokenDset, transDset, con2CDset, path, jsonlist)
    print("FINISHED")

    tokenDB.close()
    transDB.close()
    con2CDB.close()



if __name__ == '__main__':
    startTime = time.time()
    main(sys.argv)
    print("Total time: --- %s seconds ---" % (time.time() - startTime))
