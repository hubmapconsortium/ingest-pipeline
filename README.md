# HuBMAP Ingest Pipeline

## About

This repository implements the internals of the HuBMAP data repository
processing pipeline. This code is independent of the UI but works in
response to requests from the data-ingest UI backend.

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

| <strong>Request Ingest</strong>   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Description      | Cause a workflow to be applied to a dataset in the LZ. The full dataset path is computed from the data parameters.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| HTTP Method      | POST                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Example URL      | /api/hubmap/request_ingest                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| URL Parameters   | None                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Data Parameters  | provider : one of a known set of providers, e.g. 'Vanderbilt'<br> submission_id : unique identifier string for the data submission<br> process : one of a known set of process names, e.g. 'MICROSCOPY.IMS.ALL'                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Success Response | Code: 200<br> Content: {"ingest_id":"some_unique_string", "run_id":"some_other_unique_string"}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Error Responses  | Bad Request:<br>&nbsp;    Code: 400<br>   &nbsp; Content strings:<br>&nbsp; &nbsp;  "Must specify provider to request data be ingested"<br>&nbsp; &nbsp;      "Must specify sample_id to request data be ingested"<br>&nbsp; &nbsp;      "Must specify process to request data be ingested"<br>&nbsp; &nbsp;      "_NAME_ is not a known ingestion process" <br>Unauthorized:<br>&nbsp;    Code: 401<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "You are not authorized to use this resource"<br> Not Found:<br>&nbsp;    Code: 404<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "Resource not found"<br>&nbsp; &nbsp;      "Dag id _DAG_ID_ not found" <br>Server Error:<br>&nbsp;    Code: 500<br>&nbsp;    Content strings:<br>&nbsp; &nbsp;      "An unexpected problem occurred"<br>&nbsp; &nbsp;      "The request happened twice?"<br>&nbsp; &nbsp;      "Attempt to trigger run produced an error: _ERROR_" |
