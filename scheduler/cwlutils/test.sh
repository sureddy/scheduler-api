#!/bin/bash
cwl-runner --tmp-outdir-prefix=. --outdir=. --tmpdir-prefix=./tmp/ --basedir . cwl/example_workflow.cwl#main --s3creds-path=$HOME/.aws/credentials --keys cdis-test/util.html --keys cdis-test/abc 
