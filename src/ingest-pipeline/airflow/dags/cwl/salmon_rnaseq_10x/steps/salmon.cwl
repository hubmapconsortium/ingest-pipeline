cwlVersion: v1.0
class: CommandLineTool
hints:
  DockerRequirement:
    dockerPull: mruffalo/salmon-grch38:1.0.0
baseCommand: salmon
label: Run Salmon Alevin tool on FASTQ input

arguments:
  - alevin
  - "--index"
  - /opt/grch38_index
  - "--libType"
  - A
  - "--output"
  - out
  - "--chromiumV3"
  - "--tgMap"
  - /opt/Homo_sapiens.GRCh38.cdna.all.fa.gz.map

inputs:
  threads:
    type: int
    inputBinding:
      position: 0
      prefix: "-p"
  fastq_r1:
    type: File
    inputBinding:
      position: 1
      prefix: "-1"
  fastq_r2:
    type: File
    inputBinding:
      position: 2
      prefix: "-2"

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
