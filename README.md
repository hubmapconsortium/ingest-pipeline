# HuBMAP Ingest Pipeline

## About

This repository implements the internals of the HuBMAP data repository
processing pipeline. This code is independent of the UI but works in
response to requests from the data-ingest UI backend.

## Using the devtest assay type

*devtest* is a mock assay for use by developers.  It provides a testing tool controlled by a simple YAML file, allowing a developer to simulate execution of a full ingest pipeline without the need for real data.  To do a devtest run, follow this procedure.

1) Create an input dataset, for example using the ingest UI.
  - It must have a valid Source ID.
  - Its datatype must be Other -> devtest
2) Insert a control file named *test.yml* into the top-level directory of the dataset.  The file format is described below.  You may include any other files in the directory, as long as test.yml exists.
3) Submit the dataset.

Ingest operations will proceed normally from that point:
1) The state of the original dataset will change from New through Processing to QA.
2) A secondary dataset will be created, and will move through Processing to QA with an adjustable delay (see below).
3) Files specified in *test.yml* may be moved into the dataset directory of the secondary dataset.
4) All normal metadata will be returned, including extra metadata specified in *test.yml* (see below).

The format for *test.yml* is:
```
{
  # the following line is required for the submission to be properly identified at assay 'devtest'
  collectiontype: devtest,
  
  # The pipeline_exec stage will delay for this many seconds before returning (default 30 seconds)
  delay_sec: 120,
  
  # If this list is present, the listed files will be copied from the submission directory to the derived dataset.
  files_to_copy: ["file_068.bov", "file_068.doubles"],
  
  # If present, the given metadata will be returned as dataset metadata for the derived dataset.
  metadata_to_return: {
    mymessage: 'hello world',
    othermessage: 'and also this'
  }
}

```

## API

| <strong>API Test</strong>         |                                          |
|------------------|------------------------------------------|
| Description      | Test that the API is available           |
| HTTP Method      | GET                                      |
| Example URL      | /api/hubmap/test                         |
| URL Parameters   | None                                     |
| Data Parameters  | None                                     |
| Success Response | Code: 200<br> Content: {"api_is_alive":true} |
| Error Responses  | None                                     |

| <strong>Get Process Strings</strong>         |                                          |
|------------------|------------------------------------------|
| Description      | Get a list of valid process identifier keys            |
| HTTP Method      | GET                                      |
| Example URL      | /api/hubmap/get_process_strings                         |
| URL Parameters   | None                                     |
| Data Parameters  | None                                     |
| Success Response | Code: 200<br> Content: {"process_strings":[...list of keys...]} |
| Error Responses  | None                                     |

| <strong>Get Version Information</strong>         |                                          |
|------------------|------------------------------------------|
| Description      | Get API version information           |
| HTTP Method      | GET                                      |
| Example URL      | /api/hubmap/version                       |
| URL Parameters   | None                                     |
| Data Parameters  | None                                     |
| Success Response | Code: 200<br> Content: {"api":API version, "build":build version} |
| Error Responses  | None                                     |

| <strong>Request Ingest</strong>   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Description      | Cause a workflow to be applied to a dataset in the LZ. The full dataset path is computed from the data parameters.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| HTTP Method      | POST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Example URL      | /api/hubmap/request_ingest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| URL Parameters   | None                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Data Parameters  | provider : one of a known set of providers, e.g. 'Vanderbilt'<br> submission_id : unique identifier string for the data submission<br> process : one of a known set of process names, e.g. 'MICROSCOPY.IMS.ALL'                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Success Response | Code: 200<br> Content:{<br>"ingest_id":"some_unique_string",<br> "run_id":"some_other_unique_string"<br>}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Error Responses  | Bad Request:<br>&nbsp;    Code: 400<br>   &nbsp; Content strings:<br>&nbsp; &nbsp;  "Must specify provider to request data be ingested"<br>&nbsp; &nbsp;      "Must specify sample_id to request data be ingested"<br>&nbsp; &nbsp;      "Must specify process to request data be ingested"<br>&nbsp; &nbsp;      "_NAME_ is not a known ingestion process" <br>Unauthorized:<br>&nbsp;    Code: 401<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "You are not authorized to use this resource"<br> Not Found:<br>&nbsp;    Code: 404<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "Resource not found"<br>&nbsp; &nbsp;      "Dag id _DAG_ID_ not found" <br>Server Error:<br>&nbsp;    Code: 500<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "An unexpected problem occurred"<br>&nbsp; &nbsp;      "The request happened twice?"<br>&nbsp; &nbsp;      "Attempt to trigger run produced an error: _ERROR_" |
