#!/bin/bash
files=database/*/*.hdf5
for f in $files
do
  zip $f.zip $f &
done
