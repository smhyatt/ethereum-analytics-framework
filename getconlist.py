import os, json
import time
import gzip


def getContractNames(path, jsonfile):
    openFile = os.path.join(path, jsonfile)

    allList = []            # all data (list of dictionaries)
    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    CCList = []             # contract creation data list
    for di in allList:
        if 'receipt_contract_address' in di:
            CCList.append(di['receipt_contract_address'])

    return CCList



def main():
    path = "../ethereumchain"
    filelist = os.listdir(path)
    translist = []
    for file in filelist:
        filetype = (file.split("-"))
        if filetype[1] == 'transactions':
            translist.append(file)

    idx = 0
    translist.sort()

    f = open('contractlist.txt', 'a')

    for tf in translist:
        file = str(tf)
        tmplst = getContractNames(path, file)

        print(idx)
        for elm in tmplst:
           f.write(str(elm)+'\n')

        idx += 1



if __name__ == '__main__':
    startTime = time.time()
    main()
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))

