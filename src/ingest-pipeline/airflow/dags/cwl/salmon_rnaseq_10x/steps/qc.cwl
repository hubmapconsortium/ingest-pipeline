cwlVersion: v1.0
class: CommandLineTool
label: Compute QC measures
hints:
  DockerRequirement:
    dockerPull: mruffalo/scanpy:latest
baseCommand: /opt/scanpy_entry_point.py

arguments:
  - qc_checks
inputs:
  h5ad_file:
    type: File
    inputBinding:
      position: 1
outputs:
  qc_results:
    type: File
    outputBinding:
      glob: qc_results.hdf5
