FROM ubuntu:15.04
MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN apt-get update && apt-get install python-pip git awscli dnsutils -y
ADD bin/s3util bin/
