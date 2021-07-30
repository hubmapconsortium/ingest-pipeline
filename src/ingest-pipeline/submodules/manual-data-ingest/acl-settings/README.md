# Appropriate ACLs for Dataset Protection #

These ACLs can be used to establish appropriate levels of accessibility
for datasets, depending on dataset status and whether or not the dataset
contains personally identifying information.  Note that the datasets still
need to reside in the appropriate directory tree based on source, PII, and
status.  To set protections on a published dataset containing no PII one
might do:

    setfacl -b $1
    setfacl -R -M public-published.acl $1
