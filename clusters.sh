#!/bin/bash

outputpath=results/clusters
mkdir -p $outputpath

python -u cluster.py 3 3 3

git pull
git add $outputpath/*
git commit -m "Cluster pictures added by server."
git push
