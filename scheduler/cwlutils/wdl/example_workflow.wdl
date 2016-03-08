import "s3download_keys.wdl" as s3download
import "s3upload.wdl" as s3upload




workflow example {
    
    Array[String] keys
    File s3creds_path
    String bucket
    call s3download.download{
        input: keys=keys, s3creds_path=s3creds_path
    }

    call s3upload.upload as scattered_upload{
        input: s3creds_path=s3creds_path, bucket=bucket,files=s3download.download.out
    }
    
}