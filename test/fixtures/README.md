We need to think about two distinct kinds of JSON:

- In `json-ingest`, we have examples of a form which is a direct mapping from the CSV,
  plus basic metadata about the submission itself.
  This will be validated, and if there are errors, the user will be notified.
- In `json-es`, we have examples of the documents which will be indexed with Elasticsearch.
  - All the chained sample IDs will be collapsed into one multi-valued field.
  - Searchable metadata for entities earlier in the provenance chain will also be brought in.
  - The state of the entity will be indexed.
