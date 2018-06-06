package org.apache.beam.examples;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class KafkaConsumerExample {

    static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
    static final int WINDOW_SIZE = 1;  // Default window duration in minutes
    static final int WINDOW_SIZE_MILLI = 100;  // Default window duration in milliseconds
    static Logger LOG = LoggerFactory.getLogger(KafkaConsumerExample.class);

    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("streamed-word-counts")
        @Required // Set this required option to specify where to write the output.
        String getOutput();
        void setOutput(String value);

        @Description("The directory to output files to. Must end with a slash.")
        @Required
        ValueProvider<String> getOutputDirectory();
        void setOutputDirectory(ValueProvider<String> value);

        @Description("The filename prefix of the files to write to.")
        @Default.String("output")
        @Required
        ValueProvider<String> getOutputFilenamePrefix();
        void setOutputFilenamePrefix(ValueProvider<String> value);

        @Description("The shard template of the output file. Specified as repeating sequences "
                + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
                + "shard number, or number of shards respectively")
        @Default.String("W-P-SS-of-NN")
        ValueProvider<String> getOutputShardTemplate();
        void setOutputShardTemplate(ValueProvider<String> value);

        @Description("The suffix of the files to write.")
        @Default.String("")
        ValueProvider<String> getOutputFilenameSuffix();
        void setOutputFilenameSuffix(ValueProvider<String> value);


        @Description("kafka bootstrap server")
        @Default.String("localhost:9092")
        @Required // Set this required option to specify where to write the output.
        String getBootStrapServer();
        void setBootStrapServer(String value);

        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);


        @Description("Fixed window duration, in milliseconds")
        @Default.Integer(WINDOW_SIZE_MILLI)
        Integer getWindowMilliSize();
        void setWindowMilliSize(Integer value);

        @Description("Fixed number of shards to produce per window")
        @Default.Integer(1)
        Integer getNumShards();
        void setNumShards(Integer numShards);

        @Description("Consumer Group ID")
        @Default.String("my_local_beam_app")
        String getGroupID();
        void setGroupID(String groupID);
    }

    /* to run

    cp cp ~/dev/google_creds/graceful-matter-161422-7731fb242139.json ~/dev/google_creds/current.json

    or

    cp ~/dev/google_creds/lcg-bi-nonprod.json ~/dev/google_creds/current.json

    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.KafkaConsumerExample \
    -Dexec.args="--runner=DataflowRunner  \
    --project=lcg-bi-nonprod \
    --network=lcg-bi-nonprod-network \
    --subnetwork=regions/europe-west2/subnetworks/bi-nonprod-app \
    --region=europe-west1 \
    --zone=europe-west2-c	 \
    --gcpTempLocation=gs://pythian-test-bucket-ladbrokes/staging/ \
    --stagingLocation=gs://pythian-test-bucket-ladbrokes/staging/ \
    --output=gs://pythian-test-bucket-ladbrokes/output \
    --bootStrapServer=35.230.158.114:9092" \
    -Pdataflow-runner

    or run locally

    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.KafkaConsumerExample \
    -Dexec.args="--runner=DirectRunner  \
    --bootStrapServer=35.230.158.114:9092" \
    -Pdirect-runner

    */

    public static void main(String[] args) {

        //PipelineOptions options = PipelineOptionsFactory.create();

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        final String output = options.getOutput();
        final String bootstrapserver = options.getBootStrapServer(); //need to be HA
        final String groupid = options.getGroupID();

        Pipeline p = Pipeline.create(options);        // Create the Pipeline object with the options we defined above.

        KafkaIO.Read<String, String> KafkaReader = KafkaIO.<String, String>read()
                //.withBootstrapServers("localhost:9092")
                //.withBootstrapServers("35.230.158.114:9092") //need to be HA
                .withBootstrapServers(bootstrapserver)
                .withTopics(Arrays.asList("test".split(",")))

                //With earliest - it uses the earliest offset
                //last offset before switching off local/direct runner is 311384
                //next offset after switching on local/direct runner is 311385
                //using earliest works as expected - basically - new consumer groups pick up from the earliest offset and then when restarted pickup from the latest offset
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                //this does not work as expected for new consumer groups - because they don't start from the beginning!
                //.updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"latest"))
                .updateConsumerProperties(ImmutableMap.of("enable.auto.commit", (Object)"true"))

                //.updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))
                //changing this is still giving me latest offset - need to revise auto.offset.reset
                //used direct-runner
                //.updateConsumerProperties(ImmutableMap.of("group.id", "my_local_beam_app_3"))
                .updateConsumerProperties(ImmutableMap.of("group.id", groupid))
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class);

        PCollection<KafkaRecord<String, String>> WindowedKafkaMessages =
                //p.apply(KafkaReader).apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));
                //p.apply(KafkaReader).apply(Window.into(FixedWindows.of(Duration.millis(100))));
                p.apply(KafkaReader).apply(Window.into(FixedWindows.of(Duration.millis(options.getWindowMilliSize()))));


        PCollection<KV<String, String>> KafkaMessages = WindowedKafkaMessages.apply("ExtractMessages", ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String,String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KafkaRecord<String, String> kafkaRecord = c.element();
                int partitionID = kafkaRecord.getPartition();
                Long messageOffet = kafkaRecord.getOffset();
                KV<String,String> kv = kafkaRecord.getKV();
                LOG.info("partition = " + partitionID + " messageOffset = " + messageOffet + " messageKey = " + kv.getKey() + " value = " + kv.getValue());
                c.output(kv);
            }
            }));

        PCollection<String> printableOutput = KafkaMessages.apply(Values.<String>create())
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(TOKENIZER_PATTERN)) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }));
                //.apply(TextIO.write().to("wordcounts"));

        //this is overwriting the same file with latest data - incorrect
        //printableOutput.apply(new WriteOneFilePerWindow(output, options.getNumShards()));

        //need to make each write unique - using current datatime - doesn't work!
        //printableOutput.apply(new WriteOneFilePerWindow(output + System.currentTimeMillis(), options.getNumShards())); //this is overwriting the same file with latest data - incorrect

        printableOutput.apply(
                "Write File(s)",
                TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards())
                        .to(
                                new WindowedFilenamePolicy(
                                        options.getOutputDirectory(),
                                        options.getOutputFilenamePrefix(),
                                        options.getOutputShardTemplate(),
                                        options.getOutputFilenameSuffix()))
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(
                                options.getOutputDirectory(),
                                (SerializableFunction<String, ResourceId>) input ->
                                        FileBasedSink.convertToFileResourceIfPossible(input))));

        p.run().waitUntilFinish();
    }



}