#!/usr/bin/env cwl-runner


description: |
    Download files for an object storage that talks S3 
    Usage: cwl-runner <path_to_cwl_file> options
    Options:
      --processes       number of processes to perform download
      --s3creds-path    path to s3 credential file
      --signpost-url    the url to get file location given ids
      --ids             target ids to download
      --s3-url          S3 endpoint
      --keys            target key path to download, this is exclusive of ids 

requirements:
  - class: DockerRequirement
    dockerPull: quay.io/cdis/cwlutils:s3cwl
  - class: InlineJavascriptRequirement

class: CommandLineTool
id: "download"
inputs:
  - id: "#ids"
    type:
      type: array
      items: string
    default: null
    inputBinding:
      prefix: --ids

  - id: "#s3creds-path"
    type: File
    inputBinding:
      prefix: --s3creds-path

  - id: "#s3-url"
    type: string
    inputBinding:
      prefix: --s3-url
    default: null

  - id: "#signpost-url"
    type: ["null", "string"]
    inputBinding:
      prefix: --signpost-url

  - id: "#keys"
    default: null
    type:
      type: array
      items: string
    inputBinding:
      prefix: --keys

  - id: "#path"
    type: string
    inputBinding:
      prefix: --path
    default: "output"

  - id: "#processes"
    type: int
    inputBinding:
      prefix: --processes
    default: null



outputs:
  - id: "#files"
    type:
      type: array
      items: File
    outputBinding:
      glob:
        engine: node-engine.cwl
        script: |
          {
          if (inputs["keys"]) { return "output/*/*";}
          else {return "output/*"}
          }

baseCommand: ["s3util", "download"]
