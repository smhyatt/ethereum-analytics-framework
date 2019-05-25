import numpy as np
import os, json
import hashlib
import queue
import h5py
import time
import gzip
import re
import sys

global curContract
global curFromAdr
global matchName

contractDict, tokenContractDict = ({} for i in range(2)) 
curContract = ''
curFromAdr  = ''
matchName   = ''



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


    elif inputData[2:10] == '095ea7b3':     # transferFrom event
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


def getAddr(st):
    s = re.split('/|"', st)
    return str(s[len(s)-2])

def getPath(st):
    s = st.split('"')
    return str(s[len(s)-2])

def getGroup(name, node):
    global matchName
    nameList = name.split("/")
    lastElm  = nameList[len(nameList)-1]

    if lastElm == curFromAdr:
        matchName = lastElm
        return node


''' Computes a hash value of a string.
    @contract: a contract given as a string.
    returns: 32 character hash string.
'''
def computeHash(contract):
    hashObj = hashlib.md5(contract.encode())
    return hashObj.hexdigest()


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


def contractCreation(contCDB, ownership, ownerFallouts, CCList):
    global tuui_web3
    global curFromAdr
    print("Length of CCList", len(CCList))

    for lst in CCList:
        fromAdr, conAdr, inpt, timest, nonce, value = [lst[i] for i in range(len(lst))]

        inpt = inpt[2:-40]
        inptHash = computeHash(inpt)
        dataset  = np.array((conAdr, inptHash, timest, nonce, value), dtype=h5py.special_dtype(vlen=str))

        if isContract(fromAdr):
            curFromAdr = fromAdr
            node = contCDB.visititems(getGroup)

            if matchName == curFromAdr:                     # the contract is already stored under an EOA
                addr = getAddr(str(node))                   # get the path of the node
                conNode = contCDB.get(getPath(str(node)), getclass=False, getlink=False) # get the link to the node
                subgrp  = conNode.create_group(conAdr)      # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                continue

            # OBS. temporary solution
            else:                                           # the contract's EOA is not yet stored
                grp = ownerFallouts.create_group(fromAdr)   # create a subgroup for the contract
                subgrp = grp.create_group(conAdr)           # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                continue

        else:
            if fromAdr in ownership:                        # if the address is already stored in the database
                existNode = ownership.get(fromAdr, getclass=False, getlink=False)       # get the node
                if conAdr in existNode:                      # the contract is already stored
                    continue
                subgrp = existNode.create_group(conAdr)      # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                continue

            elif fromAdr not in ownership:                  # if the address is not already stored in the database
                grp = ownership.create_group(fromAdr)       # create a group for the EOA
                subgrp = grp.create_group(conAdr)            # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                continue




def sortData(path, jsonfile):

    openFile = os.path.join(path, jsonfile)
    allList, CCList, transactionList, tokenList, tmpList = ([] for i in range(5)) 

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

        elif 'receipt_contract_address' in di:  # a contract creation
            tmpList = [di['from_address'],di['receipt_contract_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
            CCList.append(tmpList)

    return (CCList, tokenList, transactionList)



def pipeline(contCDB, ownership, ownerFallouts, tokenDset, transDset, con2CDset, path, jsonlist):
    for jsonfile in jsonlist:
        fileStartTime = time.time()
        (CCList, tokenList, transactionList) = sortData(path, jsonfile)
        CCListLen = len(CCList)

        contractCreation(contCDB, ownership, ownerFallouts, CCList)
        tranRes = sortTransactions(transactionList)
        tokens  = sortTokens(tokenList)

        tokens, cons2cons, transactions = ([] for i in range(3)) 

        for el in tranRes:
            elem = el[0]
            isCall = el[1]

            if isCall:
                cons2cons.append(elem)
            else:
                transactions.append(elem)


        addTokens(tokenDset, tokens)
        addTransactions(transDset, transactions)
        addConsToCons(con2CDset, cons2cons):


        fileEndTime  = time.time() - fileStartTime
        filesVisited = open("seenfiles.txt", "a+")
        timeSpan     = open("timespan.txt", "a+")

        filesVisited.write(str(jsonfile)+'\n')
        timeSpan.write(str(fileEndTime)+", "+str(CCListLen)+", "+str(len(transactionList))+", "+str(len(tokenList))+'\n')
        print("File added", jsonfile)

        filesVisited.close()
        timeSpan.close()





def main(arg):
    val = int(arg[1])
    global contractDict
    global tokenContractDict

    jsonlist, contractLst, tokenLst = ([] for i in range(3)) 

    f1 = open('contractlist.txt', 'r').read()
    for con in f1:
        newcon = con[:-1]
        contractLst.append(newcon)

    contractDict = dict.fromkeys(contractLst)


    f2 = open('tokencontractlist.txt', 'r').read()
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
    contCFilename = 'contcdata'+ str(val) +'.hdf5'

    tokenDB = h5py.File('database/'+tokenFilename, 'a')
    transDB = h5py.File('database/'+transFilename, 'a')
    con2CDB = h5py.File('database/'+callsFilename, 'a')
    contCDB = h5py.File('database/'+contCFilename, 'a')

    ownership = contCDB.require_group('ownership')
    # for the ones that are contracts creating contracts where the EOA is not yet seen
    ownerFallouts = ownership.require_group('fallouts')

    tokenDset = tokenDB['tokens']
    transDset = transDB['transactions']
    con2CDset = con2CDB['contractTransfers']

    for tf in translist:
        file = str(tf)
        if file+'\n' in open("seenfiles.txt").read():   # check if we have already read this file before
            print("Skipped file:", file)
            continue                                    # if so, continue to the next file

        if idx < val*50:
            idx += 1
            continue

        if idx-(val*50) == 50:
            break

        jsonlist.append(file)
        idx += 1

    pipeline(val, contCDB, ownership, ownerFallouts, tokenDset, transDset, con2CDset, path, jsonlist)
    print("FINISHED")

    tokenDB.close()
    transDB.close()
    con2CDB.close()
    contCDB.close()



if __name__ == '__main__':
    startTime = time.time()
    main(sys.argv)
    print("Total time: --- %s seconds ---" % (time.time() - startTime))
