#!/usr/bin/python
import os
import math
import boto
import boto.s3.connection
import argparse

# Hacking ssl connection context. Need python 2.7.9 or higher. Because I don't have a cert with HTTPS connection.
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--access", help="access key", default='access key')
parser.add_argument("-k", "--secret", help="secret key", default='secret secret')
parser.add_argument("-b", "--bucket", help="bucket", default='bucket0')
parser.add_argument("-x", "--host", help="host", default='localhost')
parser.add_argument("-p", "--port", help="port", type=int, default=8080)
parser.add_argument("-s", "--partsize", help="partsize", type=int, default=10485760)
parser.add_argument("-f", "--file", help="file", default='file')
parser.add_argument("-o", "--object", help="object", default='file')
args = parser.parse_args()

parts = 0
n = 0

conn = boto.connect_s3(
    aws_access_key_id = args.access,
    aws_secret_access_key = args.secret,
    host = args.host,
    port = args.port,
    is_secure=True,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

bucket = conn.create_bucket(args.bucket)


# figure out how many parts
filesize = os.path.getsize(args.file)
parts = int(math.ceil(float(filesize) / float(args.partsize)))

print "  begin upload of " + args.file
print "  size " + str(filesize) + ", " + str(parts) + " parts"
part = bucket.initiate_multipart_upload(args.object)

fp = open(args.file, 'r')
for n in range(1, parts + 1):
    if (filesize - (n - 1) * args.partsize < args.partsize):
        size = filesize - (n - 1) * args.partsize
    else:
        size = args.partsize
    print "    upload part " + str(n) + " size " + str(size)
    part.upload_part_from_file(fp = fp, part_num = n, size = size)

print "  end upload"
part.complete_upload()
fp.close()
