#!/bin/bash

outputpath=results/numclusters
mkdir -p $outputpath

python -u numclusters.py

git pull
git add $outputpath/*
git commit -m "Cluster number pictures added by server."
git push
