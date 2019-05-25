#!/bin/bash
mkdir -p database/transactions

# Remember to outcomment the things you don't want to delete. 
rm -r database/transactions

echo '' > database/transactions/seenfiles.txt
echo '' > database/transactions/timespan.txt
echo '' > database/transactions/multiout.out

python invokessetup.py
