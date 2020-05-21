cwlVersion: v1.0
class: CommandLineTool
label: Filtering and normalization
hints:
  DockerRequirement:
    dockerPull: mruffalo/scanpy:latest
baseCommand: /opt/scanpy_entry_point.py

arguments:
  - filter_normalize
inputs:
  h5ad_file:
    type: File
    inputBinding:
      position: 1
outputs:
  filtered_normalized:
    type: File
    outputBinding:
      glob: filtered_normalized.h5ad
