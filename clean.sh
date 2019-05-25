#!/bin/bash
mkdir database
rm database/*
echo '' > seenfiles.txt
echo '' > timespan.txt
echo '' > multiout.out
python setup.py
