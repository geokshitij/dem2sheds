#!/bin/bash
mkdir -p WBD_unzipped
for z in WBD_*.zip; do
  name=$(basename "$z" .zip)
  mkdir -p "WBD_unzipped/$name"
  unzip -o "$z" -d "WBD_unzipped/$name"
done

