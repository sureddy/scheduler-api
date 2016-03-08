#!/usr/bin/env cwl-runner


description: |
    Upload files for an object storage that talks S3 
    Usage: cwl-runner <path_to_cwl_file> options
    Options:
      --processes       number of processes to perform upload
      --s3creds-path    path to s3 credential file
      --files           target files to upload
      --bucket          target bucket
      --s3-url          S3 endpoint

requirements:
  - class: DockerRequirement
    dockerPull: quay.io/cdis/cwlutils:s3cwl
  - class: InlineJavascriptRequirement

class: CommandLineTool

inputs:
  - id: "#files"
    type:
      type: array
      items: File
    default: null
    inputBinding:
      prefix: --files

  - id: "#s3creds-path"
    type: File
    inputBinding:
      prefix: --s3creds-path

  - id: "#bucket"
    type: string
    inputBinding:
      prefix: --bucket

  - id: "#s3-url"
    type: string
    inputBinding:
      prefix: --s3-url
    default: null

  - id: "#processes"
    type: int
    inputBinding:
      prefix: --processes
    default: null



outputs:
  - id: "#message"
    type: string
    outputBinding:
      glob: out.txt
      loadContents: true
      outputEval: $(self[0].contents)

stdout: out.txt

baseCommand: ["s3util", "upload"]
