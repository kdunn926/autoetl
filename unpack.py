#!/usr/bin/python
#
#       incrond-triggered data unpacking mechanism
#       (unzips and relocates data for ingestion)
#
#       Author: Kyle Dunn (kdunn[9][2][6][@]gmail)
#               (c) Dunn Infinite Designs LLC (2015)
#
#       This script is invoked anytime an
#       IN_CLOSE_WRITE event is detected by
#       the incrond daemon (assumed to be running)
#
#       Only the directories specified will trigger
#       the script, see a sample incrontab below:
#
#               $ incrontab -l
#               /data/stage IN_CLOSE_WRITE /data/scripts/unpack.py $@/$#
#
#       Note, the $@/$# arguments pass the
#       path and file name to this script
#       as arguments
#


from sys import argv
from os import stat
from zipfile import ZipFile 
from shutil import move
from time import time

fullFilePath = argv[1]
filename = fullFilePath.split('/')[-1]

loadingDir = "/data/loading"
metaDir = "/data/meta"

passwordFile = open('/etc/key', 'r')
theDataPassword = passwordFile.read().strip()
passwordFile.close()


def getFileStats(fullFilePath):
    # Get file metadata
    fileStatInfo = stat(fullFilePath)
    
    # Capture file metadata fields of interest
    sizeInBytes = fileStatInfo.st_size
    modificationTime = fileStatInfo.st_mtime
    creationTime = fileStatInfo.st_ctime
    return creationTime, modificationTime, sizeInBytes

def makeRecord(filename, path, code):

    fullFilePath = path + "/" + filename

    # Get the current time (GMT, epoch)
    now = int(time())

    if code == "OK":
        create, mod, size = getFileStats(fullFilePath)
    else:
        create = mod = size = "NA"

    # Build up a CSV record for this files metadata
    return "{0},{1},{2},{3},{4},{5}\n".format(now, create, filename, 
                                              size, mod, code)


statusDict = {}

with ZipFile(fullFilePath) as zf: 
    # Extract one file at-a-time to gaurd
    # against path traversal issues
    # alternatively, use zf.extractall()
    for member in zf.infolist():
        # Path traversal defense copied from
        # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
        words = member.filename.split('/')

        path = loadingDir
        for word in words[:-1]:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir, ''): 
                continue
            path = os.path.join(path, word)

        # Wrap this up to log errors, if necessary
        try:
            zf.extract(member, path, pwd=theDataPassword)
            statusDict[member] = makeRecord(member.filename, path, "OK")

        except RuntimeError as e:
            statusDict[member] = makeRecord(member.filename, path, e.message)
            pass


# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/extract', 'a')
for datafile in statusDict.keys():
    record = statusDict[datafile]
    theFile.write(record)

theFile.close()

