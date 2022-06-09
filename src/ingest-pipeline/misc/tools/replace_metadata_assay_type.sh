#!/bin/bash -ex

set -e  # really exit on error

old_assay_name='LC-MS (metabolomics)'
new_assay_name='LC-MS'
uuid=$1
echo $uuid
echo `basename "$PWD"`
if [[ `basename "$PWD"` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
fi

fname=`ls *metadata.tsv | head -1`
echo $fname

dir_prot=`stat -c '%a' .`
chmod u+w .
mkdir -p extras
extras_prot=`stat -c '%a' extras`
chmod u+w extras
prot=`stat -c '%a' $fname`
mv $fname extras/${fname}.orig
sed "s/${old_assay_name}/${new_assay_name}/" < extras/${fname}.orig > $fname
chmod $prot $fname
chmod $extras_prot extras
chmod $dir_prot .
cmp --silent $fname extras/${fname}.orig
if [ ! $? ] ; then
    echo "The edit to metadata made no change!"
    exit -1
fi
