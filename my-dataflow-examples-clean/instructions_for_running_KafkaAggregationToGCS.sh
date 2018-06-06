## instructions for KafkaAggregationToGCS DirectRunner

cd /Users/tezza/dev/dataflow/my-dataflow-examples/word-count-beam
export OUTPUT_SHARD_TEMPLATE="W-SS-NN"
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.KafkaAggregationToGCS -Dexec.args="--runner=DirectRunner  \
  --bootStrapServer=localhost:9092 \
  --groupID=6c2ec90c-65f9-11e8-adc0-fa7ae01bbebc \
  --numShards=1 \
  --outputDirectory=./YYYY-MM-DD-HH \
  --tempLocation=./${PIPELINE_NAME}/temp/ \
  --outputFilenamePrefix=windowed-file \
  --outputShardTemplate=$OUTPUT_SHARD_TEMPLATE \
  --outputFilenameSuffix=.txt" \
  -Pdirect-runner


## instructions for KafkaConsumerExample DataFlowRunner

cd /Users/tezza/dev/dataflow/my-dataflow-examples/word-count-beam
cp ~/dev/google_creds/lcg-bi-nonprod.json ~/dev/google_creds/current.json
export PIPELINE_NAME=KafkaAggregationToGCS
export GOOGLE_APPLICATION_CREDENTIALS=~/dev/google_creds/current.json
export YOUR_GCS_BUCKET=pythian-test-bucket-ladbrokes
export YOUR_PROJECT=lcg-bi-nonprod
export YOUR_NETWORK=lcg-bi-nonprod-network
export INPUT_TOPIC=projects/$YOUR_PROJECT/topics/myTopic
export YOUR_SUBNETWORK=regions/europe-west2/subnetworks/bi-nonprod-app
export OUTPUT_SHARD_TEMPLATE="W-SS-NN"
export BOOT_STRAP_SERVER=35.230.158.114:9092
export GROUP_ID=6c2ec90c-65f9-11e8-adc0-fa7ae01bbebc
export RUNNER=DataflowRunner
export REGION=europe-west1
export ZONE=europe-west2-c
export OUTPUT_FILENAME_PREFIX=windowed-file
export OUTPUT_FILENAME_SUFFIX=.txt
export NUM_SHARDS=1
export WINDOW_MILLI_SIZE=100

mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.examples.${PIPELINE_NAME}  \
-Dexec.args="--runner=${RUNNER}  \
--bootStrapServer=${BOOT_STRAP_SERVER} \
--groupID=${GROUP_ID} \
--project=${YOUR_PROJECT} \
--network=${YOUR_NETWORK} \
--subnetwork=${YOUR_SUBNETWORK} \
--region=${REGION} \
--zone=${ZONE}   \
--stagingLocation=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/staging/ \
--tempLocation=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/temp/ \
--windowMilliSize=${WINDOW_MILLI_SIZE} \
--numShards=${NUM_SHARDS} \
--outputDirectory=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/YYYY-MM-DD/HH \
--outputFilenamePrefix=${OUTPUT_FILENAME_PREFIX} \
--outputShardTemplate=$OUTPUT_SHARD_TEMPLATE \
--outputFilenameSuffix=${OUTPUT_FILENAME_SUFFIX}" \
-Pdataflow-runner

#error updating - new job is not compatible with old (as we're not aggregating etc. So I recreated and then update worked
export JOB_NAME=kafkaaggregationtogcs-tezza-0605074837-e6a5cf7b

mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.examples.${PIPELINE_NAME}  \
-Dexec.args="--runner=${RUNNER}  \
--bootStrapServer=${BOOT_STRAP_SERVER} \
--groupID=${GROUP_ID} \
--project=${YOUR_PROJECT} \
--network=${YOUR_NETWORK} \
--subnetwork=${YOUR_SUBNETWORK} \
--region=${REGION} \
--zone=${ZONE}   \
--stagingLocation=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/staging/ \
--tempLocation=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/temp/ \
--windowMilliSize=${WINDOW_MILLI_SIZE} \
--numShards=${NUM_SHARDS} \
--outputDirectory=gs://${YOUR_GCS_BUCKET}/${PIPELINE_NAME}/YYYY-MM-DD/HH \
--outputFilenamePrefix=${OUTPUT_FILENAME_PREFIX} \
--outputShardTemplate=$OUTPUT_SHARD_TEMPLATE \
--outputFilenameSuffix=${OUTPUT_FILENAME_SUFFIX} \
--jobName=$JOB_NAME \
--update" \
-Pdataflow-runner


#########################################################################################################################ß

## instructions for PubsubToText
cd /Users/tezza/dev/dataflow/DataflowTemplates
cp ~/dev/google_creds/lcg-bi-nonprod.json ~/dev/google_creds/current.json
export PIPELINE_NAME=PubsubToText
export GOOGLE_APPLICATION_CREDENTIALS=~/dev/google_creds/current.json
export YOUR_GCS_BUCKET=pythian-test-bucket-ladbrokes
export YOUR_PROJECT=lcg-bi-nonprod
export YOUR_NETWORK=lcg-bi-nonprod-network
export INPUT_TOPIC=projects/$YOUR_PROJECT/topics/myTopic
export YOUR_SUBNETWORK=regions/europe-west2/subnetworks/bi-nonprod-app

export OUTPUT_SHARD_TEMPLATE="W-SS-NN"
export JOB_NAME=pubsubtotext-tezza-0604001106-f662e155
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${YOUR_PROJECT} \
--network=${YOUR_NETWORK} \
--subnetwork=${YOUR_SUBNETWORK} \
--region=europe-west1 \
--zone=europe-west2-c   \
--stagingLocation=gs://${YOUR_GCS_BUCKET}/staging/ \
--tempLocation=gs://${YOUR_GCS_BUCKET}/temp/ \
--runner=DataflowRunner \
--windowDuration=1m \
--numShards=1 \
--inputTopic=${INPUT_TOPIC} \
--outputDirectory=gs://${YOUR_GCS_BUCKET}/YYYY-MM-DD/HH \
--outputFilenamePrefix=windowed-file \
--outputShardTemplate=$OUTPUT_SHARD_TEMPLATE \
--outputFilenameSuffix=.text \
--jobName=$JOB_NAME \
--update"