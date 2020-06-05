cwlVersion: v1.0
class: CommandLineTool
hints:
  DockerRequirement:
    dockerPull: mruffalo/salmon-grch38:20200213
baseCommand: /opt/salmon_wrapper.py
label: Run Salmon Alevin tool on FASTQ input

# arguments are hardcoded in salmon_wrapper.py

inputs:
  threads:
    type: int
    inputBinding:
      position: 0
      prefix: "--threads"
  fastq_dir:
    type: Directory
    inputBinding:
      position: 1

outputs:
  quant_mat:
    type: File
    outputBinding:
      glob: out/alevin/quants_mat.gz
  quant_mat_cols:
    type: File
    outputBinding:
      glob: out/alevin/quants_mat_cols.txt
  quant_mat_rows:
    type: File
    outputBinding:
      glob: out/alevin/quants_mat_rows.txt
  quant_tier_mat:
    type: File
    outputBinding:
      glob: out/alevin/quants_tier_mat.gz
