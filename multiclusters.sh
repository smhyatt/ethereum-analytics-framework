#!/bin/bash

outputpath=results/clusters
mkdir -p $outputpath

python -u cluster.py 5 5 5
python -u cluster.py 10 10 10
python -u cluster.py 20 20 20
python -u cluster.py 30 30 30


git pull
git add $outputpath/*
git commit -m "Cluster pictures added by server."
git push

