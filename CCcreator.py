import numpy as np
import os, json
import hashlib
import h5py
import time
import gzip
import re

contractDict = {} 
curContract = ''
curFromAdr  = ''
matchName   = ''


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
    allList, CCList, tmpList = ([] for i in range(3)) 

    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    for di in allList:
        if 'receipt_contract_address' in di:  # a contract creation
            tmpList = [di['from_address'],di['receipt_contract_address'],di['input'],di['block_timestamp'],di['nonce'],di['value']]
            CCList.append(tmpList)

    return CCList



def fileExecutor(contCDB, ownership, ownerFallouts, path, jsonlist):
    for jsonfile in jsonlist:
        fileStartTime = time.time()
        CCList = sortData(path, jsonfile)
        CCListLen = len(CCList)

        contractCreation(contCDB, ownership, ownerFallouts, CCList)

        fileEndTime  = time.time() - fileStartTime
        filesVisited = open("database/CC/seenfiles.txt", "a+")
        timeSpan     = open("database/CC/timespan.txt", "a+")

        filesVisited.write(str(jsonfile)+'\n')
        timeSpan.write(str(fileEndTime)+", "+str(CCListLen)+"\n")
        print("File added", jsonfile)

        filesVisited.close()
        timeSpan.close()




def main():
    global contractDict

    jsonlist, contractLst = ([] for i in range(2)) 

    f = open('contractlist.txt', 'r').read()
    for con in f:
        newcon = con[:-1]
        contractLst.append(newcon)

    contractDict = dict.fromkeys(contractLst)


    path = os.path.expanduser("~/chaindata/ethereum-transactions")
    filelist = os.listdir(path)
    translist = []
    for file in filelist:
        filetype = (file.split("-"))
        if filetype[1] == 'transactions':
            translist.append(file)

    idx = 0
    translist.sort()

    contCFilename = 'contcdata.hdf5'
    contCDB = h5py.File('database/CC/'+contCFilename, 'a')

    ownership = contCDB.require_group('ownership')
    # for the ones that are contracts creating contracts where the EOA is not yet seen
    ownerFallouts = ownership.require_group('fallouts')

    for tf in translist:
        file = str(tf)
        if file+'\n' in open("database/CC/seenfiles.txt").read():   # check if we have already read this file before
            print("Skipped file:", file)
            continue                                    # if so, continue to the next file


        jsonlist.append(file)
        idx += 1

    fileExecutor(contCDB, ownership, ownerFallouts, path, jsonlist)
    print("FINISHED")

    contCDB.close()



if __name__ == '__main__':
    startTime = time.time()
    main()
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))
