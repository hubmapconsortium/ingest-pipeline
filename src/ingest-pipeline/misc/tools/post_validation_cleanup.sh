#!/bin/bash -ex

uuid=$1
echo $uuid
echo `basename "$PWD"`
if [[ `basename "$PWD"` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
else
    if [ -e validation_report.txt ] ; then
	if [[ `cat validation_report.txt` == 'No errors!' ]] ; then
	    rm validation_report.txt
	else
	    echo "Validation report is not clean"
	    exit -1
	fi
    else
	echo "No validation report found"
	exit -1
    fi
    pushd extras
    for fname in *metadata.tsv.orig *contributors.tsv.orig ; do
	if [ -e ${fname} ] ; then
	    rm $fname
	else
	    echo "nothing to remove for ${fname}"
	fi
    done
    for fname in *.fastq ; do
	if [ ! -e ${fname} ] ; then
	    echo 'no fastq files found'
	    break
	fi
	if [ -e ../${fname}.gz ] ; then
	    rm $fname
        fi
    done
    popd
fi



