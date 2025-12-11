#!/bin/bash
base="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/HU2/Shape"
for i in $(seq -w 1 22); do
  wget -c "$base/WBD_${i}_HU2_Shape.zip"
done

