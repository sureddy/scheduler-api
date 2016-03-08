task download {
    Array[String]? ids
    String? signpost_url
    String? s3_url
    File s3creds_path
    runtime {
        docker: "quay.io/cdis/cwlutils:s3cwl"
    }
    command {
        s3util download --ids ${sep=' '  ids}  --s3creds-path ${s3creds_path} ${"--s3-url" + s3_url}
    }
    output {
        String out = read_string(stdout())
    }
}


workflow test {
  call download {
    input: s3_url="https://s3.amazonaws.com"
  }
}