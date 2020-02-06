#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
# TODO: Make main.py executable?
baseCommand: ['python', '/main.py', '--output_dir', '.', '--input_dir']
hints:
  DockerRequirement:
    dockerPull: hubmap/portal-container-h5ad-to-arrow:0.0.1
inputs:
  input_directory:
    type: Directory
    inputBinding:
        position: 6
outputs:
  csv:
    type: File
    outputBinding:
      glob: '*.csv'
  json:
    type: File
    outputBinding:
      glob: '*.json'
# NOTE: The Arrow output here does not match our fixture in the source repo.
# The JSON and CSV are generated from the Arrow, so I am very confident
# the content is semantically correct, but why is it different at the byte level?
  arrow:
    type: File
    outputBinding:
      glob: '*.arrow'
