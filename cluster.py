from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import h5py
import time
import sys
import re


ownerConsList = []
ownerHashList = []


def getAddr(st):
    s = re.split('/|"', st)
    return str(s[len(s)-2])


def ownerContracts(name, node):
    global ownerConsList

    ownerAdr = getAddr(name)
    for key, val in node.attrs.items():
        conAdr  = str(val[0])
        insList = [ownerAdr, conAdr]
        ownerConsList.append(insList)


def ownerHashes(name, node):
    global ownerHashList

    ownerAdr = getAddr(name)
    for key, val in node.attrs.items():
        hashval = str(val[1])
        insList = [ownerAdr, hashval]
        ownerHashList.append(insList)


def plotClustering(clusters, X, theType, picName):
    kmeans = KMeans(n_clusters=clusters, init='k-means++')
    kmeans.fit(X) 
    print(kmeans.cluster_centers_) 
    print(kmeans.labels_)  

    plt.scatter(X[:,0], X[:,1], c=kmeans.labels_, cmap='rainbow')  
    plt.scatter(kmeans.cluster_centers_[:,0] ,kmeans.cluster_centers_[:,1], color='black')  

    # Set Log scale
    plt.xscale('log')
    plt.yscale('log')

    # Set the axis labels.
    if theType=='hashperowner':  # [number of contracts per owner, number unique contracts per owner] 
        plt.xlabel('The number of contracts per owner.') 
        plt.ylabel('The number of unique contracts per owner.')
    if theType=='hashallcopies': # [number of times contract occurs, unique owners using contract]
        plt.xlabel('The number of times a contract occurs more than once.') 
        plt.ylabel('The number of unique owners using each copied contract.')
    else:
        plt.xlabel('The number of times the same amount of contracts occurs.') 
        plt.ylabel('The number of contracts per owner.')
    plt.savefig('results/clusters/'+'cluster++'+str(picName)+str(clusters)+'.png')
    plt.close()


def createContractX():
    contractOccurence = []
    finalResult = []
    owners = []

    for x in ownerConsList:
        owners.append(x[0])

    uniqueOwners = list(set(owners))
    for x in uniqueOwners:
        contractOccurence.append(owners.count(x))

    for x in list(set(contractOccurence[::-1])):
        finalResult.append([contractOccurence.count(x),x])

    return np.array(finalResult)


def createHashAnalysisDicts():
    hashOccursD, ownerOccursD = ({} for i in range(2)) 

    for d in ownerHashList:
        ownerAdr = d[0]
        hashStr  = d[1]

        if ownerAdr not in ownerOccursD:
            ownerOccursD[ownerAdr] = [0] # [number of contracts per owner, unique contracts per owner (all hashstrings)]
        if ownerAdr in ownerOccursD:
            ownerValLst = ownerOccursD[ownerAdr]
            ownerValLst[0] += 1
            if hashStr not in ownerValLst:
                ownerValLst.append(hashStr)

        if hashStr not in hashOccursD:
            hashOccursD[hashStr] = [0]  # [number of times contract occurs, unique owners using contract (all hashstrings)]
        if hashStr in hashOccursD:
            hashValLst = hashOccursD[hashStr]
            hashValLst[0] += 1
            if ownerAdr not in hashValLst:
                hashValLst.append(ownerAdr) 

    return (ownerOccursD, hashOccursD)


def createHashX(dic):
    result = []

    for k in dic:
        valLst  = dic[k]
        numUniq = len(valLst[1:])
        instLst = [valLst[0], numUniq]
        result.append(instLst)

    return np.array(result)


def getCopiesX(dic):
    result = []

    for k in dic:
        valLst    = dic[k]
        numOwners = valLst[0]
        numUniq   = len(valLst[1:])
        if numOwners > 1:      # it is a copy, because more than 2 occur. 
            instLst = [numOwners, numUniq]
            result.append(instLst)

    return np.array(result)


def main(arg):
    nCluster1 = int(arg[1])
    nCluster2 = int(arg[2])
    nCluster3 = int(arg[3])

    filename = 'database/CC/contcdata.hdf5'
    f = h5py.File(filename, 'r')

    f.visititems(ownerHashes)
    f.visititems(ownerContracts)

    (ownerOccursD, hashOccursD) = createHashAnalysisDicts()

    X1 = createHashX(ownerOccursD)  # [number of contracts per owner, number unique contracts per owner] 
    X2 = getCopiesX(hashOccursD)    # [number of times a contract occurs more than once, number unique owners using contract]
    X3 = createContractX()          # [number of contracts per owner, occurences of each number of contracts per owner]

    plotClustering(nCluster1, X1, 'hashperowner', '1-clusters-contracts-per-owners-unique-contracts-per-owner')
    plotClustering(nCluster2, X2, 'hashallcopies', '2-clusters-total-copies-unique-owners-for-each-contract')
    plotClustering(nCluster3, X3, 'consperowner', '3-clusters-contracts-per-owner-and-occurrences-of-each-number-of-contracts-per-owner')

    f.close()

if __name__ == '__main__':
    startTime = time.time()
    main(sys.argv)
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))

