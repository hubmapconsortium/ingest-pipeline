cwlVersion: v1.0
class: CommandLineTool
label: Dimensionality reduction and clustering
hints:
  DockerRequirement:
    dockerPull: mruffalo/scanpy:latest
baseCommand: /opt/scanpy_entry_point.py

arguments:
  - dim_reduce_cluster
inputs:
  h5ad_file:
    type: File
    inputBinding:
      position: 1
outputs:
  dim_reduced_clustered:
    type: File
    outputBinding:
      glob: dim_reduced_clustered.h5ad
  umap_pdf:
    type: File
    outputBinding:
      glob: umap_by_leiden_cluster.pdf
