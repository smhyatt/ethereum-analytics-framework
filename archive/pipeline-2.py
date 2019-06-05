import eth_utils as ut
from web3 import Web3
import numpy as np
import threading
import os, json
import hashlib
import queue
import h5py
import time
import gzip
import re

global curContract
global curFromAdr
global matchName

curContract = ''
curFromAdr  = ''
matchName   = ''
exitFlag1 = 0
exitFlag2 = 0

fileQueue   = queue.Queue()
transQueue  = queue.Queue()
resultQueue = queue.Queue()

writeLock  = threading.Lock()
contCLock  = threading.Lock()
transQLock = threading.Lock()

connectionLock = threading.Lock()

phase1Lock = threading.Lock()
phase2Lock = threading.Lock()
phase3Lock = threading.Lock()


tuui_infura_url = "https://mainnet.infura.io/v3/859a0c3062564f2fa3fb978a3d465e77"
tuui_web3 = Web3(Web3.HTTPProvider(tuui_infura_url))

juui_infura_url = "https://mainnet.infura.io/v3/8b030401a2ed407d8b27afc6ebbb4373"
juui_web3 = Web3(Web3.HTTPProvider(juui_infura_url))

guui_infura_url = "https://mainnet.infura.io/v3/32ea6d001fb04b549fdf9aaf0f9253ca"
guui_web3 = Web3(Web3.HTTPProvider(guui_infura_url))



class myThread(threading.Thread):
    def __init__(self, threadID, contCDB, ownership, ownerFallouts, tokenDset, transDset):
        threading.Thread.__init__(self)
        self.threadID  = threadID
        self.contCDB   = contCDB
        self.ownership = ownership
        self.ownerFallouts = ownerFallouts
        self.tokenDset = tokenDset
        self.transDset = transDset

    def run(self):
        if self.threadID < 10:
            pipeline(self.threadID, self.contCDB, self.ownership, self.ownerFallouts, self.tokenDset, self.transDset)
        else:
            sortTransactionsWrapper(self.threadID, self.tokenDset, self.transDset)


def getRecipient(threadID, inputData):
    spender = None
    nonce = None
    isEoa = False
    value = 0
    recipient = None

    if inputData[2:10] == 'a9059cbb':       # transfer event
        recipient = '0x'+inputData[34:74]   # = to address for token holder
        vdata = inputData[75:138]           # = amount
        if re.search(r'\d', vdata):
            value = int(vdata, 16)

        if threadID % 2 == 0:
            isEOA = juuiIsEOACheck(recipient)
        else:
            isEOA = guuiIsEOACheck(recipient)


    elif inputData[2:10] == '23b872dd':     # transferFrom event
        inputData[34:74]                    # = token contract address
        vdata = inputData[75:138]           # = amount (hex string)
        if re.search(r'\d', vdata):
            value = int(vdata, 16)
        recipient = '0x'+inputData[162:202] # = token user address

        if threadID % 2 == 0:
            isEOA = juuiIsEOACheck(recipient)
        else:
            isEOA = guuiIsEOACheck(recipient)


    elif inputData[2:10] == '095ea7b3':     # transferFrom event
        spender = inputData[34:74]          # = spender
        vdata   = inputData[75:138]         # = amount
        if re.search(r'\d', vdata):
            value = int(vdata, 16)
    else:
        recipient = None

    return (recipient, spender, str(value), isEoa)





def sortTransactions(threadID, dsetTok, dsetTran, transaction):        #dset, falloutDset, transactionList):
    global transQueue
    global resultQueue
    idx = 0
    # print("Length of transactionList:", len(transactionList))

    fromAdr, toAdr, inpt, timest, nonce, fstVal = [transaction[i] for i in range(len(transaction))]


    (recipient, spender, sndVal, endReceiverIsEOA) = getRecipient(threadID, inpt)

    if threadID % 2 == 0:
        isEOA = juuiIsEOACheck(toAdr)
    else:
        isEOA = guuiIsEOACheck(toAdr)

    if isEOA == True:     # case of EOA to EOA
        toCon = False
    else:
        toCon = True      # case of EOA to contract

    if recipient is not None:
        tokendata = np.array((str(fromAdr), str(toAdr), str(recipient), str(spender), str(toCon), str(endReceiverIsEOA), str(timest),
        str(nonce), str(fstVal), str(sndVal)), dtype=h5py.special_dtype(vlen=str))
        resultQueue.put((tokendata, True))
    else:
        transdata = np.array((fromAdr, toAdr, toCon, timest, nonce, fstVal), dtype=h5py.special_dtype(vlen=str))
        resultQueue.put((transdata, False))




def sortTransactionsWrapper(threadID, tokenDset, transDset):
    global transQueue
    global resultQueue
    global juui_web3
    global guui_web3
    idx = 0

    while not exitFlag2:
        transQLock.acquire()
        try:
            transaction = transQueue.get(block=True, timeout=0.3)
            if idx % 2000 == 0:
                print("Thread and index in sortTransactions:", threadID, idx)
                print("transQueue len", len(transQueue) )
            idx += 1
            transQLock.release()
        except:
            idx = 0
            transQLock.release()
            continue

        try:
            sortTransactions(threadID, tokenDset, transDset, transaction)
            transQueue.task_done()
        except Exception as e:
            print("error", e)
            connectionLock.acquire()
            transQueue.put(transaction)
            transQueue.task_done()
            transQLock.release()
            if threadID % 2 == 0:
                if juui_web3.isConnected() == False:
                    juui_web3 = Web3(Web3.HTTPProvider(juui_infura_url))
            else:
                if guui_web3.isConnected() == False:
                    guui_web3 = Web3(Web3.HTTPProvider(guui_infura_url))
            connectionLock.release()



def addTokens(dsetTok, tokens):
    tokLen = len(tokens)

    dsetTok.resize(dsetTok.shape[0]+tokLen, axis=0)
    print("token resize")
    idx1 = -tokLen
    for token in tokens:
        dsetTok[idx1, :] = token
        if idx1 % 1000 == 0:
            print("Token insertion number", idx1)
        idx1 += 1



def addTransactions(dsetTran, transactions):
    traLen = len(transactions)

    dsetTran.resize(dsetTran.shape[0]+traLen, axis=0)
    print("transaction resize")
    idx2 = -traLen
    for transaction in transactions:
        dsetTran[idx2, :] = transaction
        if idx2 % 1000 == 0:
            print("Transaction insertion number", idx2)
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


def tuuiIsEOACheck(addr):
    if ut.is_checksum_address(addr) == False:
        addr = ut.to_checksum_address(addr)
    code = tuui_web3.eth.getCode(addr)

    if code == "0x":
        return True

    if not code == "0x":
        return False


def juuiIsEOACheck(addr):
    if ut.is_checksum_address(addr) == False:
        addr = ut.to_checksum_address(addr)
    code = juui_web3.eth.getCode(addr)

    if code == "0x":
        return True

    if not code == "0x":
        return False


def guuiIsEOACheck(addr):
    if ut.is_checksum_address(addr) == False:
        addr = ut.to_checksum_address(addr)
    code = guui_web3.eth.getCode(addr)

    if code == "0x":
        return True

    if not code == "0x":
        return False


# def contractCreationWrapper(contCDB, ownership, ownerFallouts, CCList):
#     while 1:
#         break


def contractCreation(contCDB, ownership, ownerFallouts, CCList):
    global tuui_web3
    global curFromAdr
    print("Length of CCList", len(CCList))
    idx = 0

    for lst in CCList:
        if idx%100==0:
            print("contractCreation number", idx)
        idx += 1

        fromAdr, conAdr, inpt, timest, nonce, value = [lst[i] for i in range(len(lst))]

        inpt = inpt[2:-40]
        inptHash = computeHash(inpt)
        contCLock.acquire()
        dataset  = np.array((conAdr, inptHash, timest, nonce, value), dtype=h5py.special_dtype(vlen=str))

        # if tuui_web3.isConnected() == False:
        #     tuui_infura_url = "https://mainnet.infura.io/v3/859a0c3062564f2fa3fb978a3d465e77"
        #     tuui_web3 = Web3(Web3.HTTPProvider(tuui_infura_url))

        try:
            tuuiIsEOACheckBool = tuuiIsEOACheck(fromAdr)
        except:
            print("error in contract wrapper")
            tuui_web3 = Web3(Web3.HTTPProvider(tuui_infura_url))
            tuuiIsEOACheckBool = tuuiIsEOACheck(fromAdr)

        if tuuiIsEOACheckBool == True:
            if fromAdr in ownership:                        # if the address is already stored in the database
                existNode = ownership.get(fromAdr, getclass=False, getlink=False)       # get the node
                if conAdr in existNode:                      # the contract is already stored
                    contCLock.release()
                    continue
                subgrp = existNode.create_group(conAdr)      # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                contCLock.release()
                continue

            elif fromAdr not in ownership:                  # if the address is not already stored in the database
                grp = ownership.create_group(fromAdr)       # create a group for the EOA
                subgrp = grp.create_group(conAdr)            # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                contCLock.release()
                continue

        else:
            node = contCDB.visititems(getGroup)

            if matchName == curFromAdr:                     # the contract is already stored under an EOA
                addr = getAddr(str(node))                   # get the path of the node
                conNode = contCDB.get(getPath(str(node)), getclass=False, getlink=False)      # get the link to the node
                subgrp  = conNode.create_group(conAdr)       # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                contCLock.release()
                continue

            # OBS. temporary solution
            else:                                           # the contract's EOA is not yet stored
                grp = ownerFallouts.create_group(fromAdr)   # create a subgroup for the contract
                subgrp = grp.create_group(conAdr)           # create a subgroup for the contract
                subgrp.attrs.create('att'+conAdr, dataset, shape=None, dtype=None)       # attribute with time and nonce
                contCLock.release()
                continue
        contCLock.release()





def sortData(path, jsonfile):

    openFile = os.path.join(path, jsonfile)

    allList = []            # all data (list of dictionaries)
    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    CCList = []             # contract creation data list
    transactionList = []    # transactions data list
    tmpList = []

    for di in allList:
        if 'to_address' in di:    # regular transaction
            tmpList = [di['from_address'],di['to_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
            transactionList.append(tmpList)

        elif 'receipt_contract_address' in di:
            tmpList = [di['from_address'],di['receipt_contract_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
            CCList.append(tmpList)

    return (CCList, transactionList)



def pipeline(threadID, contCDB, ownership, ownerFallouts, tokenDset, transDset):
    global exitFlag2
    global fileQueue
    global transQueue
    localExitFlag = 0

    if ((tuui_web3.isConnected() == False) or (juui_web3.isConnected() == False)
        or (guui_web3.isConnected() == False)):
        print("Infura is not connected.")

    while not exitFlag1:
        phase1Lock.acquire()

        try:
            print("locked file", threadID)
            (path, jsonfile) = fileQueue.get(block=True, timeout=0.3)
            fileQueue.task_done()
            if fileQueue.empty():
                localExitFlag = 1
            fileStartTime = time.time()
        except:
            print("unlocked file", threadID)
            phase1Lock.release()
            continue

        (CCList, transactionList) = sortData(path, jsonfile)


        contractCreation(contCDB, ownership, ownerFallouts, CCList)

        phase2Lock.acquire()
        phase1Lock.release()
        print("after phase1Lock", threadID)
        print("before phase2Lock", threadID)

        for lst in transactionList:
            transQueue.put(lst)

        print("Waiting for transQueue.", threadID)
        transQueue.join()
        print("transQueue done.", threadID)

        exitFlag2 = localExitFlag

        tokens = []
        transactions = []
        print("getting from resultQueue", threadID)
        while not resultQueue.empty():
            (elem, isToken) = resultQueue.get()
            if len(resultQueue) % 2000 == 0:
                print("len of resultQueue", len(resultQueue))
            if isToken:
                tokens.append(elem)
            else:
                transactions.append(elem)
        print("done with resultQueue", threadID)

        # (transactions, tokens) = sortTransactions(tokenDset, transDset, transactionList)

        phase3Lock.acquire()
        print("phase3Lock set", threadID)
        phase2Lock.release()
        print("after phase2Lock", threadID)

        addTokens(tokenDset, tokens)
        addTransactions(transDset, transactions)

        fileEndTime  = time.time() - fileStartTime
        filesVisited = open("seenfiles.txt", "a+")
        timeSpan     = open("timespan.txt", "a+")

        filesVisited.write(str(jsonfile)+'\n')
        timeSpan.write(str(fileEndTime)+'\n')
        print("File added", jsonfile)

        filesVisited.close()
        timeSpan.close()

        phase3Lock.release()
        print("phase3Lock released", threadID)




def main():
    global fileQueue
    global exitFlag1
    
    path = "/home/dikuprojects/eth/ethchain/ethereumchain"
    filelist = os.listdir(path)
    translist = []
    for file in filelist:
        filetype = (file.split("-"))
        if filetype[1] == 'transactions':
            translist.append(file)

    idx = 0
    translist.sort()

    tokenFilename = 'tokendata.hdf5'
    transFilename = 'transdata.hdf5'
    contCFilename = 'contcdata.hdf5'

    tokenDB = h5py.File('database/'+tokenFilename, 'a')
    transDB = h5py.File('database/'+transFilename, 'a')
    contCDB = h5py.File('database/'+contCFilename, 'a')

    ownership = contCDB.require_group('ownership')
    # for the ones that are contracts creating contracts where the EOA is not yet seen
    ownerFallouts = ownership.require_group('fallouts')

    tokenDset = tokenDB['tokens']
    transDset = transDB['transactions']


    for tf in translist:
        file = str(tf)
        if file+'\n' in open("seenfiles.txt").read():  # check if we have already read this file before
            print("Skipped file:", file)
            continue                        # if so, continue to the next file

        if idx == 3:
            break

        fileQueue.put((path, file))

        idx += 1

    threads = []
    thread0 = myThread(0, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread1 = myThread(1, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread2 = myThread(2, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread3 = myThread(10, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread4 = myThread(11, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread5 = myThread(12, contCDB, ownership, ownerFallouts, tokenDset, transDset)
    thread6 = myThread(13, contCDB, ownership, ownerFallouts, tokenDset, transDset)

    thread0.start()
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    threads.append(thread0)
    threads.append(thread1)
    threads.append(thread2)
    threads.append(thread3)
    threads.append(thread4)
    threads.append(thread5)
    threads.append(thread6)


    fileQueue.join()
    exitFlag1 = 1
    print("FINISHED")

    for th in threads:
        print("before join")
        th.join()
        print("after join")


    tokenDB.close()
    transDB.close()
    contCDB.close()



if __name__ == '__main__':
    startTime = time.time()
    main()
    print("Total time: --- %s seconds ---" % (time.time() - startTime))
