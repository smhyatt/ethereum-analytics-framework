#!/bin/bash
files=database/transactions/*.hdf5
for f in $files
do
  zip $f.zip $f &
done
