import os, json
import time
import gzip



def getTokenContracts(path, jsonfile):

    openFile = os.path.join(path, jsonfile)

    allList = []            # all data (list of dictionaries)
    with gzip.GzipFile(openFile, 'r') as jfile:
        line = jfile.readline().decode('utf-8')

        while line:
            data = json.loads(line)
            allList.append(data)
            line = jfile.readline().decode('utf-8')

    conToConList = [] 

    for di in allList:
        if 'to_address' in di:
            toAdr = di['to_address']
            if not toAdr == '0x0000000000000000000000000000000000000000':
                conToConList.append(di['to_address'])

    return conToConList



def main():
    path = os.path.expanduser("~/chaindata/ethereum-token-transfers")
    
    filelist = os.listdir(path)

    translist = []
    for f in filelist:
        filetype = (f.split("-"))
        if (filetype[1] == 'token') and (filetype[2] == 'transfers'):
            translist.append(f)

    idx = 0
    translist.sort()
    tokConList = []

    for tf in translist:
        file = str(tf)

        tokConList += getTokenContracts(path, file)

        idx += 1


    tokConList = list(dict.fromkeys(tokConList))
    with open('tokencontractlist.txt', 'a') as conAdr:
        for a in tokConList:
           conAdr.write(str(a)+"\n")


if __name__ == '__main__':
    startTime = time.time()
    main()
    totalTime = time.time() - startTime
    totalTime = float("{0:.3f}".format(totalTime))
    print("Total time: {0} seconds.".format(totalTime))

