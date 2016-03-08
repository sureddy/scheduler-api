task download {
    Array[String] keys
    String? signpost_url
    String? s3_url
    File s3creds_path
    runtime {
        docker: "quay.io/cdis/cwlutils:s3cwl"
    }
    command {
        s3util download --keys ${sep=' ' keys}  --s3creds-path ${s3creds_path} ${"--s3-url " + s3_url} --path "output"
    }
    output {
        Array[File] out = glob("output/*/*")
    }
}


workflow test {
  call download {
    input: s3_url="https://s3.amazonaws.com"
  }
}