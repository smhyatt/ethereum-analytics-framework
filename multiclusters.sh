#!/bin/bash

outputpath=results/clusters
mkdir -p $outputpath

python -u cluster.py 3 3 3
python -u cluster.py 8 8 8
python -u cluster.py 15 15 15
python -u cluster.py 25 25 25
python -u cluster.py 50 50 50
python -u cluster.py 75 75 75
python -u cluster.py 100 100 100
python -u cluster.py 250 250 250
python -u cluster.py 500 500 500
python -u cluster.py 750 750 750
python -u cluster.py 1000 1000 1000
python -u cluster.py 1500 1500 1500 
python -u cluster.py 2000 2000 2000

git pull
git add $outputpath/*
git commit -m "Cluster pictures added by server."
git push

