#!/home/hdiuser/.conda/envs/autoetl/bin/python
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
#               /data/ingest IN_CLOSE_WRITE /data/scripts/push.py $@/$#
#
#       Note, the $@/$# arguments pass the
#       path and file name to this script
#       as arguments
#

from sys import argv, stdout
from datetime import date
from hashlib import md5
from os.path import isfile
from os import devnull
from time import sleep, time
from azure.storage.blob import BlobService
from azure.common import AzureMissingResourceHttpError, AzureHttpError
from subprocess import Popen, STDOUT, PIPE
from json import loads
import re

metaDir = "/data/meta"

azureAccount = 'plsdatalake'
ingestContainer = 'frisco'
productionContainer = 'kdunn-test'

# This functionality is being pushed
# upstream in the ETL process
#archiveContainer = 'kdunn-test'

azureKeyLocation = '/etc/hadoop/conf/key'

hadoopEdgeNode = "frisco-ssh.azurehdinsight.net"
hiveServer2 = "hn0-frisco.jmlhoa5f5zfenakxzzq1hcslzh.bx.internal.cloudapp.net"

ddlFile = "/data/scripts/tableDefs.json"
beeline = "/usr/bin/beeline"

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
'Vaccines',
'Vitals'
]

dataSetDdl = {}
with open(ddlFile) as f:
    dataSetDdl = loads(f.read().replace("\n", ""))
    f.close()

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
    return hash.digest(), lines

fullFilePath = argv[1]
filename = fullFilePath.split('/')[-1]

# Extract the dataset type by dropping
# the file extension
dataSetType = filename.split('.')[0]

if dataSetType not in validDataSets:
    # Quietly ignore the erroneous files
    exit(0)

# Get the full file path by stripping
# off the last token (filename) and 
# appending RowCounts.txt
rowCountFullFilePath = "/".join(fullFilePath.split('/')[:-1]) + "/RowCounts.txt"

# A boolean for ensuring actual row
# counts  the metadata in RowCounts.txt
doesMatch = False

md5Checksum, countedRows = computeMd5AndLines(fullFilePath)

if dataSetType != "Clients":
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
        print "|", dataSetType, "|", dataSet, "|"
        # Find the data set of interest
        if dataSet == dataSetType:
            print expectedRows, countedRows
            # Compare the records (excluding header row)
            doesMatch = (int(expectedRows) == int(countedRows - 1))
            break

result = None
            
# Only proceed if we have a valid data or metadata file
if doesMatch or dataSetType == "Clients":

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

    # Create full paths for the location
    targetIngestFullPath = "{0}/{1}.txt".format(targetIngestPath, targetFile)
    #targetArchiveFullPath = "{0}/{1}".format(dataSetType, targetFile)

    # Ensure a clean slate for pushing the new data set
    try:
        azureStorage.delete_blob(ingestContainer, targetIngestFullPath)
    except AzureMissingResourceHttpError:
        pass

    # Try to put the blob out in the wild, provide MD5 for error
    # checking since M$ didn't feel the need to implement a return
    # code for this function

    # On further testing, the "content_md5" is only for header rather
    # than the actual blob content - have to wait for these APIs to mature
    try:
        azureStorage.put_block_blob_from_path(ingestContainer,
                                              targetIngestFullPath,
                                              fullFilePath,
                                              #content_md5=md5Checksum.encode('base64').strip(),
                                              max_connections=5)
    except AzureHttpError as e:
        result = "Ingest-Failed:" + e.message.split(".")[0]


    # Create a list of queries for Hive
    hiveQueries = []

    sortedByString = "SORTED BY(GenPatientID)"
    if dataSetType == "Clients":
        sortedByString = ""

    # Create a template external table
    # and populate it with specifics
    # for a given data set type
    hiveCreateExtTable =\
    """
    DROP TABLE pls.{dType}_stg ; \n
    CREATE EXTERNAL TABLE pls.{dType}_stg 
    ( {ddl} )
    CLUSTERED BY(GenClientID) {sortString} INTO 32 BUCKETS 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
    STORED AS TEXTFILE LOCATION 'wasb://{container}@{account}/{dType}' 
    TBLPROPERTIES("skip.header.line.count"="1");
    """.format(dType=dataSetType, 
               ddl=dataSetDdl[dataSetType],
               sortString=sortedByString,
               container=ingestContainer, 
               account=azureAccount + ".blob.core.windows.net")
               #path=targetIngestFullPath)

    hiveQueries.append(hiveCreateExtTable)

    # These data sources are full-extracts, truncate before load
    # As it turns out, Hive isn't smart enough to drop data
    # from these external/"unmanaged" tables
    #if dataSetType in ['Providers', 'Patients', 'Clients']:
    #    hiveQueries.append("TRUNCATE TABLE pls.{0}_dev ;".format(dataSetType))

    if dataSetType in ['Providers', 'Patients', 'Clients']:
        try:
            azureStorage.delete_blob(ingestContainer, dataSetType)
        except AzureMissingResourceHttpError:
            pass

    hiveQueries.append("LOAD DATA INPATH '/{path}' INTO TABLE pls.{t}_stg ;".format(path=targetIngestFullPath,
                                                                                    t=dataSetType))

    hiveQueries.append("INSERT INTO TABLE pls.{0}_dev SELECT * FROM pls.{0}_stg ;".format(dataSetType))

    hiveQueries.append("SELECT COUNT(*) FROM pls.{0}_dev ;".format(dataSetType))

    # Hide SSH output
    devNull = open(devnull)

    # Open a secure tunnel to channel the beeline
    # connection through
    ssh = Popen(["ssh", hadoopEdgeNode, "-L10001:{hs2}:10001".format(hs2=hiveServer2)],
                stdout=devNull, stderr=devNull)

    # TODO - call out to Popen() and have a beeline Hive client execute
    # the CREATE EXTERNAL TABLE and do an INSERT INTO ... SELECT * FROM ...
    p = Popen(beeline, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)

    # Note: Azure's HiveServer2 default config seems to only allow HTTP 
    # transport mode, this is unfortunate since the major Python-Thrift 
    # connector (PyHive) would have been useful rather than using Popen + beeline
    hiveConnectString = "!connect jdbc:hive2://localhost:10001/pls;transportMode=http {u} {p}".format(u="etl",
                                                                                                      p="etl")

    # Initiate the connection
    with p.stdin as hiveCli:
        hiveCli.write(hiveConnectString + "\n")

        # Execute the loading process (finally)
        for q in hiveQueries:
            #print "hive < ", q
            hiveCli.write(q + "\n")

        #print "OUT:", p.stdout.readlines()
        #print "ERR:", p.stderr.readlines()
        #stdout.flush()

        hiveCli.write("!exit \n")
    
        hiveCli.close()
        
    # Gracefully exit the beeline shell
    p.wait()

    # Close the tunnel
    ssh.terminate()
    ssh.wait()

    stdout = p.stdout.readlines()
    stderr = p.stderr.readlines()

    devNull.close()

    #print "OUT:", "\n".join(stdout)
    #print "ERR:", "\n".join(stderr)
    #print "\n\n----\n"

    print stdout[-3].split()[1], " records"

    # TODO - scrape the STDOUT for the number 
    # of records inserted for logging

    # Archive it as well -- this functionality has been relocated
    # earlier in the ETL process, entire ZIP archives will be created
    # rather than per-set TXT archives

    # On further testing, the "content_md5" is only for header rather
    # than the actual blob content - have to wait for these APIs to mature
    #try:
    #    azureStorage.put_block_blob_from_path(archiveContainer,
    #                                          targetArchiveFullPath,
    #                                          fullFilePath,
    #                                          #content_md5=md5Checksum.encode('base64').strip(),
    #                                          max_connections=5)
    #except AzureHttpError as e:
    #    if result is not None:
    #        result = result + " and Archive-Failed:" + e.message.split(".")[0]
    #    else:
    #        result = "Archive-Failed:" + e.message.split(".")[0]
else:
    result = "record count mismatch"


# Get the current time (GMT, epoch)
nowTime = int(time())

# Build up a CSV record for this files metadata
record = "{0},{1},{2},{3},{4}\n".format(nowTime, filename, countedRows-1, 
                                        md5Checksum.encode('base64').strip(), result)

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
