cwlVersion: v1.0
class: CommandLineTool
label: Convert Alevin sparse output to anndata.AnnData object, save as h5ad
hints:
  DockerRequirement:
    dockerPull: mruffalo/scanpy:latest
baseCommand: /opt/alevin_to_anndata.py

inputs:
  quant_mat:
    type: File
    inputBinding:
      position: 1
  quant_mat_cols:
    type: File
    inputBinding:
      position: 2
  quant_mat_rows:
    type: File
    inputBinding:
      position: 3
outputs:
  h5ad_file:
    type: File
    outputBinding:
      glob: out.h5ad
