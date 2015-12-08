#!/usr/bin/python
#
#       incrond-triggered data unpacking mechanism
#       (unzips and relocates data for ingestion)
#
#       Author: Kyle Dunn (kdunn[9][2][6][@]gmail)
#               (c) Dunn Infinite Designs LLC (2015)
#
#       This script is invoked anytime an
#       IN_CREATE event is detected by
#       the incrond daemon (assumed to be running)
#
#       Only the directories specified will trigger
#       the script, see a sample incrontab below:
#
#               $ incrontab -l
#               /data/stage IN_CREATE /data/scripts/unpack.py $@/$#
#
#       Note, the $@/$# arguments pass the
#       path and file name to this script
#       as arguments
#


from sys import argv
from os import stat, devnull
from zipfile import ZipFile 
from shutil import move
from time import time
from subprocess import Popen, STDOUT

fullFilePath = argv[1]
filename = fullFilePath.split('/')[-1]

unzipUtil = "/usr/bin/7z"
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

def makeRecord(filename, path, code, startTime):

    fullFilePath = path + "/" + filename

    # Get the current time (GMT, epoch)
    now = int(time())

    # Track unzip elapsed time
    runTime = (now - startTime)

    if code == "OK":
        create, mod, size = getFileStats(fullFilePath)
    else:
        create = mod = size = "NA"

    # Build up a CSV record for this files metadata
    return "{0},{1},{2},{3},{4},{5},{6}\n".format(now, create, filename, 
                                                  size, mod, runTime, code)

# Moving this to the begining to avoid stalling loads
# waiting for this file to compare row counts
unzipCommand = "{prog} x {fname} {member} -o{dest} -p{passwd}".format(prog=unzipUtil,
                                                                      fname=fullFilePath,
                                                                      member='RowCounts.txt',
                                                                      dest=loadingDir,
                                                                      passwd=theDataPassword)

# Used to supress output
devNull = open(devnull, 'w')
p = Popen(unzipCommand, shell=True, stdout=devNull, stderr=STDOUT)
p.wait()


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

        if member.filename == 'RowCounts.txt':
            continue

        # Wrap this up to log errors, if necessary
        try:
            # This doesn't work with AES-encrypted archives,
            # throws an RuntimeError -- "bad password"
            #zf.extract(member, path, pwd=theDataPassword)

            # Get the current time (GMT, epoch)
            startTime = int(time())

            unzipCommand = "{prog} x {fname} {member} -o{dest} -p{passwd}".format(prog=unzipUtil,
                                                                                  fname=fullFilePath,
                                                                                  member=member.filename,
                                                                                  dest=path,
                                                                                  passwd=theDataPassword)
            #print unzipCommand

            p = Popen(unzipCommand, shell=True, stdout=devNull, stderr=STDOUT)
            p.wait()

            if p.returncode == 0:
                statusDict[member] = makeRecord(member.filename, path, "OK", startTime)

                todoFile = "touch {p}/{f}.todo".format(p=path, f=member.filename.split(".")[0])
                t = Popen(todoFile.split())
                t.wait()
            else:
                statusDict[member] = makeRecord(member.filename, path, p.returncode, startTime)
            #print "Extracted", member.filename, "to", path

        except OSError as e:
            #print "Caught exception for", member.filename, e[0]
            statusDict[member] = makeRecord(member.filename, path, e[0])
            pass


# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/extract', 'a')
for datafile in statusDict.keys():
    record = statusDict[datafile]
    theFile.write(record)

theFile.close()

