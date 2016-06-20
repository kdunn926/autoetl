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

from azure.storage.blob import BlobService
from azure.common import AzureMissingResourceHttpError, AzureHttpError

stagingDir = "/data/staging"
metaDir = "/data/meta"
logRoot = "/data/logs"

azureAccount = 'phaprodarchive001'
ingestContainer = 'phaprodarchivepls'
azureKeyLocation = '/etc/hadoop/conf/keyArchive'


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

if "MD5" in filename.upper() or "DOCX" in filename.upper():
    # Queitly ignore the checksum and manifest files
    exit(0)

# Get the current time (GMT, epoch)
startTime = int(time())

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

logFile = logRoot + "/intake-{0}.log".format(str(startTime))

theLog = open(logFile, 'w+')

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

        # the super secret location
        accountKeyFile = open(azureKeyLocation, 'r')
        accountKey = accountKeyFile.read().strip()
        accountKeyFile.close()

        # Get a handle on the Azure Blob Storage account
        azureStorage = BlobService(account_name=azureAccount,
                                   account_key=accountKey)

        checksumFilename = md5FullFilePath + ".md5"

        # Ensure a clean slate for pushing the new data set
        try:
            azureStorage.delete_blob(ingestContainer, filename)
            theLog.write("Existing ingest data blob found, deleting it\n\n")
            theLog.flush()

            azureStorage.delete_blob(ingestContainer, filename.split(".")[0] + ".md5")
            theLog.write("Existing ingest checksum blob found, deleting it\n\n")
            theLog.flush()
        except AzureMissingResourceHttpError:
            pass

        # Try to put the blob out in the wild, provide MD5 for error
        # checking since M$ didn't feel the need to implement a return
        # code for this function

        # On further testing, the "content_md5" is only for header rather
        # than the actual blob content - have to wait for these APIs to mature
        try:
            theLog.write("Writing data to Blob {3} to {0}:{1}/{2}\n".format(azureAccount, ingestContainer, filename, stagingDir+"/"+filename))

            azureStorage.put_block_blob_from_path(ingestContainer,
                                                  filename,
                                                  stagingDir+"/"+filename,
                                                  #content_md5=md5Checksum.encode('base64').strip(),
                                                  max_connections=5)
            theLog.write("Wrote data to Blob\n")
            sleep(5)

            theLog.write("Writing md5 to Blob {3} to {0}:{1}/{2}\n".format(azureAccount, ingestContainer, filename.split(".")[0] + ".md5", md5FullFilePath)) 

            azureStorage.put_block_blob_from_path(ingestContainer,
                                                  filename.split(".")[0] + ".md5",
                                                  md5FullFilePath,
                                                  #content_md5=md5Checksum.encode('base64').strip(),
                                                  max_connections=5)

            theLog.write("Wrote md5 to Blob\n") 

            theLog.write("Data blob and checksum both successfully archived to {0}:{1}.\n\n".format(azureAccount, ingestContainer))
            theLog.flush()

            result = "OK"
        except OSError as e:
            theLog.write(e)
            result = e

        except:
            theLog.write("Failed to archive one or both blobs.\n\n")
            result = "ARCHIVE FAILED"


    except Error as e:

        result = e.message

else:

    result = "CHECKSUM MISMATCH"

# Get the current time (GMT, epoch)
endTime = int(time())

runTime = (endTime - startTime)

theLog.close()

# Build up a CSV record for this files metadata
record = "{0},{1},{2},{3},{4},{5},{6},{7}\n".format(startTime, creationTime, filename, 
                                                    sizeInBytes, modificationTime, runTime,
                                                    md5sum, result)

# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/stage', 'a')
theFile.write(record)
theFile.close()

