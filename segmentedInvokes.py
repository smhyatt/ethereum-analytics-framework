import numpy as np
import h5py
import time
import re

curFromAdr = ''
matchName  = ''


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


def getConToCon(callDset):
    callDset = np.array(callDset)
    workLst = []

    for i in callDset:
        fromAdr = i[0]
        toAdr   = i[1]
        endAdr  = i[2]
        toCon   = i[4]
        endEOA  = i[5]
        timeSt  = i[6]
        # sndVal  = i[8:9]
        if (toCon == 'True') and (endEOA == 'False'):
            insrtLst = [toAdr, endAdr, timeSt]
            workLst.append(insrtLst)

    return workLst.sort(key=lambda x: x[2])


def createTree(callLst, invokDB):
    global curFromAdr

    for lst in callLst:
        fromAdr, toAdr, timeSt = [lst[i] for i in range(len(lst))]

        curFromAdr = fromAdr                # global current from address is se
        node = invokDB.visititems(getGroup) # checks if the fromAdr exists in the hole database
        print(node)

        if matchName == curFromAdr:         # there is a match
            addr   = getAddr(str(node))     # get the path of the node
            curCon = invokDB.get(getPath(str(node)), getclass=False, getlink=False) # get the link to the node
            try:
                curCon[toAdr]
                continue
            except:
                curCon.create_group(toAdr)        # create a subgroup for the contract

        else:
            grp = invokDB.create_group(fromAdr)   # create a group for the sending contract
            grp.create_group(toAdr)               # create a subgroup for the receiving contract
            continue



# def main():
#     invokFilename = 'database/trees/invokestree.hdf5'
#     invokDB = h5py.File(invokFilename, 'a')
#     calltree = invokDB.require_group('calltree')

    # filename = 'database/transactions/callsdata'+str(i)+'.hdf5'
    # callsDB = h5py.File(filename, 'r')
    # callsDset = callsDB['contractTransfers']
    # callsDB.close()

    # callLst = getConToCon(callsDset)
    # createTree(callLst, calltree)

    # invokDB.close()



# Does not require pre-sorted databases
# def main():
#     callsDset = []

#     for i in range(15):
#         filename = 'database/transactions/callsdata'+str(i)+'.hdf5'
#         callsDB = h5py.File(filename, 'r')
#         callsDset += callsDB['contractTransfers']
#         callsDB.close()

#     invokFilename = 'database/framework/invokestree.hdf5'
#     invokDB = h5py.File(invokFilename, 'a')
#     calltree = invokDB.require_group('calltree')

#     callLst = getConToCon(callsDset)
#     createTree(callLst, calltree)

#     invokDB.close()



# Requires pre-sorted databases
def main():
    invokFilename = 'database/framework/invokestree.hdf5'
    invokDB = h5py.File(invokFilename, 'a')
    calltree = invokDB.require_group('calltree')

    for i in range(15):
        filename = 'database/transactions/callsdata'+str(i)+'.hdf5'
        callsDB = h5py.File(filename, 'r')
        callsDset = callsDB['contractTransfers']
        callsDB.close()

        callLst = getConToCon(callsDset)
        createTree(callLst, calltree)

    invokDB.close()



if __name__ == '__main__':
    startTime = time.time()
    main()
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))


