#!/usr/bin/env cwl-runner

- id: "#echo"
  class: CommandLineTool
  requirements:
  # have to use docker so that cwltool will use tmpdir specified by input args
  - class: DockerRequirement
    dockerPull: quay.io/cdis/cwlutils:s3cwl
  inputs:
    - id: "#echo-in"
      type: File
      label: "Message"
      description: "The message to print"
      inputBinding: {}
  outputs:
    - id: "#echo-out"
      type: File
      label: "Printed Message"
      description: "The file containing the message"
      outputBinding:
        glob: messageout.txt

  baseCommand: echo
  stdout: messageout.txt


- id: "#main"
  class: Workflow
  label: "example workflow"
  description: "Example workflow to download files, print, and upload"

  requirements:
    - class: ScatterFeatureRequirement

  inputs: 
    - id: "#s3creds-path"
      type: File
      inputBinding:
        prefix: --s3creds-path
      default: null


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



  outputs:
    - id: "#message"
      source: "#upload.message"
      type: string


  steps:
    - id: "#download"
      run: {import : "s3download.cwl"}
      inputs:
        - id: "#download.keys"
          source: "#keys"
        - id: "#download.s3creds-path"
          source: "#s3creds-path"
      outputs:
        - id: "#download.files"
    - id: "#test"
      run: {import: "#echo"}
      scatter: "#echo.echo-in"
      inputs:
        - id: "#echo.echo-in"
          source: "#download.files"
      outputs:
        - id: "#echo.echo-out"
    - id: "#upload"
      run: {import: "s3upload.cwl"}
      inputs:
        - id: "#upload.bucket"
          default: "cdis-test"
        - id: "#upload.files"
          source: "#echo.echo-out"
        - id: "#download.s3creds-path"
          source: "#s3creds-path"
      outputs:
        - id: "#upload.message"



