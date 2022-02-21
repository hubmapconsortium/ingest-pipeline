#!/bin/bash -ex

old_assay_name='MALDI-IMS-neg'
new_assay_name='MALDI-IMS'
uuid=$1
echo $uuid
echo `basename "$PWD"`
if [[ `basename "$PWD"` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
fi

fname=`ls *metadata.tsv | head -1`
echo $fname

mkdir -p extras
mv $fname extras/${fname}.orig
sed "s/${old_assay_name}/${new_assay_name}/" < extras/${fname}.orig > $fname
