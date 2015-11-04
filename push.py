#!/usr/bin/python
#
#       incrond-triggered data upload to Azure Blob
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
#               /data/ingest IN_CLOSE_WRITE /data/scripts/ingest.py $@/$#
#
#       Note, the $@/$# arguments pass the
#       path and file name to this script
#       as arguments
#

from sys import argv
from datetime import date
from hashlib import md5
from os.path import isfile
from time import sleep, time
from azure.storage.blob import BlobService

metaDir = "/data/meta"

azureAccount = 'plsinsightdata'
ingestContainer = 'sunnyday'
archiveContainer = 'plsinsightdata'
azureKeyLocation = '/etc/hadoop/conf/key'

# This is the path where the data will be staged
# in the ingest container (i.e. HDFS-visible location)
# in the HDInsight cluster for Hive external tables
targetIngestPath = "tmp/hive/hdiuser/ingest"

validDataSets = [
'Allergies',
'Appointments',
'Clients',
'Encounters',
'FillRates',
'Medications',
'Orders',
'PatientDemographics',
'Problems',
'Providers',
'Results',
'RowCounts',
'Vaccines',
'Vitals'
]

def computeMd5AndLines(fname):
    hash = md5()
    lines = 0
    with open(fname, "rb") as f:
    # Read 4096 bytes at a time to
    # support reading large files
        for chunk in iter(lambda: f.read(4096), b""):
            # Tabulate the newlines in this chunk
            lines = lines + chunk.count('\x0a')

            hash.update(chunk)
    return hash.hexdigest(), lines

fullFilePath = argv[1]
filename = fullFilePath.split('/')[-1]

# Extract the dataset type by dropping
# the file extension
dataSetType = filename.split('.')[0]

if dataSetType not in validDataSets:
    # Queitly ignore the erroneous files
    exit(0)

# Get the full file path by stripping
# off the last token (filename) and 
# appending RowCounts.txt
rowCountFullFilePath = "/".join([fullFilePath.split('/')[:-1]) + "/RowCounts.txt"

# A boolean for ensuring actual row
# counts match the metadata in RowCounts.txt
doesMatch = False

md5Checksum = None
countedRows = -1
if dataSetType != "RowCounts":
    md5Checksum, countedRows = computeMd5AndLines(fullFilePath)

    # Wait until row count file has also landed
    while True:
        if isfile(rowCountFullFilePath):
            break

        sleep(10)

    # Open the row counts file and read its contents
    rowCountsFile = open(rowCountFullFilePath, 'r')
    allCounts = rowCountsFile.readlines()
    rowCountsFile.close()

    # Loop through all the record counts
    for c in allCounts:
        # Split the lines on the delimiter
        dataSet, expectedRows = c.split("|")
        # Find the data set of interest
        if dataSet == dataSetType:
            # Compare the records
            doesMatch = (int(expectedRows) == int(countedRows))
            break
            
# Only proceed if we have a 
# valid data or metadata file
if doesMatch or dataSetType == "RowCounts":

    # Read in the Azure account key from
    # the super secret location
    accountKeyFile = open(azureKeyLocation, 'r')
    accountKey = accountKeyFile.read().strip()
    accountKeyFile.close()

    # Get a handle on the Azure Blob Storage account
    azureStorage = BlobService(account_name=azureAccount, 
                               account_key=accountKey)

    # Create a datestring for the filenames
    dateString = date.today().strftime("%Y%m%d")

    # Create a filename string (e.g. Allergies20151103)
    targetFile = "{0}{1}".format(dataSetType, dateString)

    # Create full file paths for the two upload locations
    targetIngestFullPath = "{0}/{1}.txt".format(targetIngestPath, targetFile)
    targetArchiveFullPath = "{0}/{1}".format(dataSetType, targetFile)

    # Try to put the blob out in the wild, provide MD5 for error
    # checking since M$ didn't feel the need to implement a return
    # code for this function
    azureStorage.put_block_blob_from_path(ingestContainer,
                                          targetIngestFullPath,
                                          sourceFilePath,
                                          content_md5=md5Checksum,
                                          max_connections=8)

    hiveExtTableQuery =\
    """
    CREATE OR REPLACE EXTERNAL TABLE pls.{dType}_stg ( LIKE pls.{dType}_dev )
    CLUSTERED BY(GenClientID) SORTED BY(GenPatientID) INTO 32 BUCKETS
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE LOCATION 'wasb://{container}@{account}/{path}'
    TBLPROPERTIES("skip.header.line.count"="1");
    """.format(dType=dataSetType, 
               container=ingestContainer, 
               account=azureAccount, 
               path=targetIngestFullPath)

    hiveInsertQuery = "INSERT INTO pls.{0}_dev SELECT * FROM pls.{0}_stg ;".format(dataSetType)
    # TODO - call out to Popen() and have a beeline Hive client execute
    # the CREATE EXTERNAL TABLE and do an INSERT INTO ... SELECT * FROM ...

    # Archive it as well
    azureStorage.put_block_blob_from_path(archiveContainer,
                                          targetArchiveFullPath,
                                          sourceFilePath,
                                          content_md5=md5Checksum,
                                          max_connections=8)


# Get the current time (GMT, epoch)
nowTime = int(time())

# Build up a CSV record for this files metadata
record = "{0},{1},{2},{3},{4}\n".format(nowTime, filename, countedRows, md5Checksum, result)

# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/insert', 'a')
theFile.write(record)
theFile.close()

# List all containers in this account
"""
for c in azureStorage.list_containers():
    print c.name, c.url

    # List all blobs in the container of interest
    if c.name == azureContainer:
        for b in azureStorage.list_blobs(c.name):
            print b.name, b.url

exit(0)
""";
