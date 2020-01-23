cwlVersion: v1.0
class: CommandLineTool
label: Compute differentially expressed genes between each cluster and the rest
hints:
  DockerRequirement:
    dockerPull: mruffalo/scanpy:latest
baseCommand: /opt/scanpy_entry_point.py

arguments:
  - compute_cluster_marker_genes
inputs:
  h5ad_file:
    type: File
    inputBinding:
      position: 1
outputs:
  cluster_marker_genes:
    type: File
    outputBinding:
      glob: cluster_marker_genes.h5ad
  marker_gene_plot_t_test:
    type: File
    outputBinding:
      glob: marker_genes_by_cluster_t_test.pdf
  marker_gene_plot_logreg:
    type: File
    outputBinding:
      glob: marker_genes_by_cluster_logreg.pdf
