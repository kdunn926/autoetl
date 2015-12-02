#!/usr/bin/python
#
#       incrond-triggered data upload to Azure Blob
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
#               /data/ingest IN_CREATE /usr/bin/python /data/scripts/push.py $@/$#
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
logRoot = "/data/logs/"

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
beeline = "env JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64 /usr/bin/beeline"

# This is the path where the data will be staged
# in the ingest container (i.e. HDFS-visible location)
# in the HDInsight cluster for Hive external tables
targetIngestPath = "tmp/hive"

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

# Get the current time (GMT, epoch)
startTime = int(time())


# This is the full source DDL 
# (including empty columns)
dataSetDdl = {}

def stringClean(theString):
    return theString.lstrip()\
                    .rstrip()\
                    .strip()\
                    .replace('"', "")

# This DDL excludes the empty columns
insertDdl = {}
with open(ddlFile) as f:
    allTheFields = f.readlines()
    f.close()

    #dataSetDdl = loads("".join(allTheFields).replace("\n", "").replace("--", ""))
    allTablesAndFields = "".join(allTheFields).replace("}", "").replace("{", "").split('",')
    for tAndF in allTablesAndFields:
        table, fields = tAndF.split(":") 

        # Clean up the string and tokenize fields
        allFields = stringClean(fields).replace(",", "").split("\n")
    
        validFields = [f.lstrip().replace("--", "") for f in allFields]

        # Filter out empty tokens 
        fieldString = ", ".join([v for v in validFields if v != ""])
        dataSetDdl[stringClean(table)] = fieldString

        # Beeline doesn't like DDL with commented fields
        # and fails to parse if it is left in

        # This filters commented columns and extracts just the column name
        # for generating a statement like INSERT INTO someTable SELECT colA, colB, colN FROM

        # Clean up the string and tokenize fields
        fields = stringClean(fields).split("\n")
    
        # Filter out disable fields
        validFields = [f.lstrip() for f in fields if "--" not in f]

        # Filter out empty tokens, split each token for 
        # only column name and join them with a column
        fieldString = ", ".join([v.split()[0] for v in validFields if v != ""])
        insertDdl[stringClean(table)] = fieldString

#print "D-DDL", dataSetDdl
#print "I-DDL", insertDdl

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

logFile = logRoot + "/push-{0}.log".format(dataSetType + "-" + str(startTime))

theLog = open(logFile, 'w+')

theLog.write("Beginning ouput log\n")
theLog.flush()

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
        # Find the data set of interest
        if dataSet == dataSetType:
            # Compare the records (excluding header row)
            doesMatch = (int(expectedRows) == int(countedRows - 1))
            break

result = None
            
# Only proceed if we have a valid data or metadata file
if doesMatch or dataSetType == "Clients":

    theLog.write("Record counts match, proceeding with load\n\n")
    theLog.flush()

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
        theLog.write("Existing ingest blob found, deleting it\n\n")
        theLog.flush()
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
        theLog.write("Uploaded blob to ingest container : {0}\n".format(ingestContainer))
        theLog.flush()
    except AzureHttpError as e:
        result = "Ingest-Failed:" + e.message.split(".")[0]
        theLog.write("Upload exception: {0}\n\n".format(result))
        theLog.flush()


    # Create a list of queries for Hive
    hiveQueries = []

    sortedByString = "SORTED BY(GenPatientID)"
    if dataSetType == "Clients" or dataSetType == "Providers":
        sortedByString = ""

    # Create a template external table
    # and populate it with specifics
    # for a given data set type
    hiveCreateExtTable =\
    """
    DROP TABLE pls.kdunn_{dType}_stg ; \n
    CREATE EXTERNAL TABLE pls.kdunn_{dType}_stg 
    ( {ddl} )
    CLUSTERED BY(GenClientID) {sortString} INTO 32 BUCKETS 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
    STORED AS TEXTFILE LOCATION 'wasb://{container}@{account}/pls/stage/{dType}' 
    TBLPROPERTIES("skip.header.line.count"="1");
    """.format(dType=dataSetType, 
               ddl=dataSetDdl[dataSetType],
               sortString=sortedByString,
               container=productionContainer, 
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
            theLog.write("Truncated {0} via Blob deletion\n\n".format(dataSetType))
            theLog.flush()
        except AzureMissingResourceHttpError:
            pass

    loadStgQuery = "LOAD DATA INPATH '/{path}' INTO TABLE pls.kdunn_{t}_stg ;".format(path=targetIngestFullPath,
                                                                                t=dataSetType)
    hiveQueries.append(loadStgQuery)

    loadDevQuery = "INSERT INTO TABLE pls.kdunn_{0}_dev SELECT {1} FROM pls.kdunn_{0}_stg ;".format(dataSetType,
                                                                                        insertDdl[dataSetType])
    hiveQueries.append(loadDevQuery)

    hiveQueries.append("SELECT COUNT(*) FROM pls.kdunn_{0}_stg ;".format(dataSetType))

    # Hide SSH output
    devNull = open(devnull)

    # Open a secure tunnel to channel the beeline
    # connection through
    ssh = Popen(["ssh", hadoopEdgeNode, "-L10001:{hs2}:10001".format(hs2=hiveServer2)],
                stdout=devNull, stderr=devNull)

    theLog.write("Opening SSH tunnel to edge node {0}\n\n".format(hadoopEdgeNode))
    theLog.flush()

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
        theLog.write("Connecting to Hiveserver2: " + hiveConnectString + "\n")
        theLog.flush()

        # Execute the loading process (finally)
        for q in hiveQueries:
            #print "hive < ", q
            hiveCli.write(q + "\n")
            theLog.write("Executing Hive query: \n")
            theLog.write(q + "\n")
            theLog.flush()

        #print "OUT:", p.stdout.readlines()
        #print "ERR:", p.stderr.readlines()
        #stdout.flush()

        hiveCli.write("!exit \n")
    
        hiveCli.close()

    theLog.write("Exited Beeline CLI\n")
    theLog.flush()
        
    # Gracefully exit the beeline shell
    p.wait()

    # Close the tunnel
    ssh.terminate()
    ssh.wait()

    stdout = p.stdout.readlines()
    stderr = p.stderr.readlines()

    devNull.close()

    theLog.write("OUT:" + "\n".join(stdout))
    theLog.flush()
    theLog.write("ERR:" + "\n".join(stderr))
    theLog.flush()
    #print "ERR:", "\n".join(stderr)
    #print "\n\n----\n"

    result = stdout[-3].split()[1]
    #print result,  "records"

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

# Time the whole process
runTime = (nowTime - startTime)

# Build up a CSV record for this files metadata
record = "{0},{1},{2},{3},{4},{5}\n".format(nowTime, runTime, filename, 
                                            md5Checksum.encode('base64').strip(), 
                                            countedRows - 1, result)

# Push the record to a tempfile for now TODO: append to MD file
theFile = open(metaDir + '/insert', 'a')
theFile.write(record)
theFile.close()

theLog.write("\n\npush.py completed\n")
theLog.close()

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
