task upload {
    Array[File] files
    File s3creds_path
    String bucket
    String? s3_url
    runtime {
        docker: "quay.io/cdis/cwlutils:s3cwl"
    }
    command {
        s3util upload --files ${default='' sep=' ' files} --bucket ${bucket} --s3creds-path ${s3creds_path} ${"--s3-url" + s3_url}
    }
    output {
        String out = read_string(stdout())
        String err = read_string(stderr())
    }
}


workflow test {
  call upload {
    input: s3_url="https://s3.amazonaws.com"
  }
}