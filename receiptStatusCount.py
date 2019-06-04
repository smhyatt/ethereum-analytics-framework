import numpy as np
import os, json
import time
import gzip


def receiptStatusCnt(path, jsonfile):
    openFile = os.path.join(path, jsonfile)

    totalCnt = 0
    allList, receiptStatusLst = ([] for i in range(2)) 

    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    for di in allList:
        totalCnt += 1
        if 'receipt_status' in di:
            status = di['receipt_status']
            receiptStatusLst.append(status)

    a = np.array(receiptStatusLst)
    unique, counts = np.unique(a, return_counts=True)
    res = dict(zip(unique, counts))

    return (res, totalCnt)


def main():
    path = os.path.expanduser("~/chaindata/ethereum-transactions")
    resDic = {}
    translist, totFail, totSucc, totCnt = ([] for i in range(4)) 
    
    filelist = os.listdir(path)

    for f in filelist:
        filetype = (f.split("-"))
        if filetype[1] == 'transactions':
            translist.append(f)

    idx = 0
    translist.sort()

    for tf in translist:
        file = str(tf)

        (resDic, totalCnt) = receiptStatusCnt(path, file)
        totCnt.append(totalCnt)
        
        if resDic:
            failStat = resDic['0'] 
            succStat = resDic['1'] 
            totFail.append(failStat)
            totSucc.append(succStat)

        idx += 1

    totalTran = sum(totCnt)
    totalSucc = sum(totSucc)
    totalFail = sum(totFail)

    print("The total number of transactions:", totalTran)

    totalReceipts    = totalSucc+totalFail
    totalReceiptsPct = (totalReceipts/totalTran)*100
    print("{0:.2f}% of transactions contain a receipt_status.".format(totalReceiptsPct))

    totalSuccessPct = (totalSucc/totalReceipts)*100
    totalFailedPct  = (totalFail/totalReceipts)*100
    print("Within transactions including a receipt_status: {0:.2f}% are successful and {1:.2f}% are failed.".format(totalSuccessPct, totalFailedPct))


if __name__ == '__main__':
    startTime = time.time()
    main()
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))
