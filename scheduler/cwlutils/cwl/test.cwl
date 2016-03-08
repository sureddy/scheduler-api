class: CommandLineTool
inputs:
  - id: "#echo-in"
    type: string
    label: "Message"
    description: "The message to print"
    default: "Hello World"
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