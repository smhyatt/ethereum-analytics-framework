#!/bin/bash
files=database/*/*.zip
for f in $files
do
  unzip $f &
done
