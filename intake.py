#!/usr/bin/python
#
#	incrond-triggered data intake mechanism
#       (performs MD5 checksum validation and staging)
#
#	Author: Kyle Dunn (kdunn[9][2][6][@]gmail)
#	        (c) Dunn Infinite Designs LLC (2015)
#
#	This script is invoked anytime an 
#	IN_CLOSE_WRITE event is detected by
#       the incrond daemon (assumed to be running)
#       
#       Only the directories specified will trigger
#       the script, see a sample incrontab below:
#
# 		$ incrontab -l
# 		/data/inbound IN_CLOSE_WRITE /data/scripts/intake.py $@/$# 
#
#	Note, the $@/$# arguments pass the 
#	path and file name to this script
#       as arguments
#

from sys import argv
from os import stat
from zipfile import ZipFile 
from hashlib import md5
from shutil import move, Error
from os.path import isfile
from time import sleep, time

stagingDir = "/data/staging"
metaDir = "/data/meta"

def computeMd5(fname):
    hash = md5()
    with open(fname, "rb") as f:
        # Read 4096 bytes at a time to 
        # support hashing large files
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash.hexdigest() 

def validateChecksums(md5sum, md5FullFilePath):
    lines = []
    with open(md5FullFilePath) as f:
        lines = f.readlines()
        f.close()

    # Baseline checksum is on the last line
    # of the file, and the zero-th token
    # (space delimited)
    baselineChecksum = lines[-1].split(' ')[0]

    return (baselineChecksum == md5sum)

fullFilePath = argv[1]
filename = fullFilePath.split('/')[-1]

if "MD5" in filename.upper():
    # Queitly ignore the checksum files
    exit(0)

# Get the current time (GMT, epoch)
nowTime = int(time())

# Get file metadata
fileStatInfo = stat(fullFilePath)

# Capture file metadata fields of interest
sizeInBytes = fileStatInfo.st_size
modificationTime = fileStatInfo.st_mtime
creationTime = fileStatInfo.st_ctime

# Compute an MD5 sum for comparison
md5sum = computeMd5(fullFilePath)

# Get the full file path minus extension
md5FullFilePath = fullFilePath.split('.')[0]

# Wait until checksum file has also landed
while True:
    if isfile(md5FullFilePath + ".md5"):
	md5FullFilePath = md5FullFilePath + ".md5"
        break
    elif isfile(md5FullFilePath + ".MD5"):
	md5FullFilePath = md5FullFilePath + ".MD5"
        break
    elif isfile(md5FullFilePath + ".Md5"):
	md5FullFilePath = md5FullFilePath + ".Md5"
        break
    elif isfile(md5FullFilePath + ".mD5"):
	md5FullFilePath = md5FullFilePath + ".mD5"
        break

    sleep(10)

if validateChecksums(md5sum, md5FullFilePath):

    try:
        move(fullFilePath, stagingDir)

        result = "OK"

    except Error as e:

        result = e.message

else:

    result = "CHECKSUM MISMATCH"

# Build up a CSV record for this files metadata
record = "{0},{1},{2},{3},{4},{5},{6}\n".format(nowTime, creationTime, filename, 
                                                sizeInBytes, modificationTime, 
                                                md5sum, result)

# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/stage', 'a')
theFile.write(record)
theFile.close()

