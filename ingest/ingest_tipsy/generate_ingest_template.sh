#!/bin/bash 
touch "ingest_all_cosmo.sh"
# Information about the data
DATA_PATH=/disk3/jortiz16/MichaelDatasets/Romulus/romulus8.256gst2.bwBH/
IORD=iord_2
GRP=amiga.grp
SIMULATION_PREFIX=romulus8.256gst2.bwBH.
SNAPSHOT_LIST=(000045 000054 000072 000102)

# Information about the relation that will be stored in Myria
USER_NAME=jortiz
PROGRAM_NAME=romulustest

#clearing old file
> ingest_all_cosmo.sh

for VARIABLE in "${SNAPSHOT_LIST[@]}"
do
	touch "ingest_cosmo$VARIABLE.json"
	echo -e "{ \n\
    	\"grpFilename\": \"$DATA_PATH$SIMULATION_PREFIX$VARIABLE.$GRP\",\n\
    	\"iorderFilename\": \"$DATA_PATH$SIMULATION_PREFIX$VARIABLE.$IORD\",\n\
    	\"relationKey\": {\n\
        	\"programName\": \"$PROGRAM_NAME\",\n\
        	\"relationName\": \"cosmo$VARIABLE\",\n\
        	\"userName\": \"$USER_NAME\"\n\
    	},\n\
    	\"tipsyFilename\": \"$DATA_PATH$SIMULATION_PREFIX$VARIABLE\"\n\
	}" > "ingest_cosmo$VARIABLE.json"
	echo -e "curl -i -XPOST https://rest.myria.cs.washington.edu:1776/dataset/tipsy -H \"Content-type: application/json\"  -d @./ingest_cosmo${VARIABLE}.json\n" >> "ingest_all_cosmo.sh"
done