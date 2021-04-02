#!/bin/bash -ex

uuid=$1
echo $uuid
echo `basename "$PWD"`
if [[ `basename "$PWD"` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
else
    mkdir -p extras
    for fname in *metadata.tsv *contributors.tsv ; do
	if [ -e ${fname} ] ; then
	    mv $fname extras/${fname}.orig
	else
	    echo "nothing to move for ${fname}"
	fi
    done
    for fname in *.fastq ; do
	if [ ! -e ${fname} ] ; then
	    echo 'no fastq files found'
	    break
	fi
	if [ -e ${fname}.gz ] ; then
	    mv $fname extras
	else
	    gzip $fname
        fi
    done
fi
metafiles_tar="/tmp/build_tree.tar"
pushd ..
for fname in `tar -tf $metafiles_tar | grep $uuid` ; do
    echo $fname
    tar -xvf $metafiles_tar $fname
done
popd


