#!/bin/bash -ex

uuid=$1
echo $uuid
echo `basename "$PWD"`
if [[ `basename "$PWD"` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
else
    pushd extras
    for fname in *metadata.tsv.orig *contributors.tsv.orig ; do
	echo rm $fname
    done
    for fname in *.fastq ; do
	if [ ! -e ${fname} ] ; then
	    echo 'no fastq files found'
	    break
	fi
	if [ -e ../${fname}.gz ] ; then
	    echo rm $fname
        fi
    done
    popd
fi



