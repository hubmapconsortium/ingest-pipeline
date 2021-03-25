#!/bin/bash -ex

uuid=$1
echo $uuid
echo `basename $PWD`
if [[ `basename $PWD` != $uuid ]]; then
   echo "run this from the $uuid directory"
   exit -1
else
    mkdir -p extras
    for fname in *metadata.tsv *contributors.tsv ; do
	mv $fname extras/${fname}.orig
    done
    for fname in *.fastq ; do
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


