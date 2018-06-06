/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link a_MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class a_MinimalWordCount {

    static Logger LOG = LoggerFactory.getLogger(a_MinimalWordCount.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        // TextIO.read() returns a PCollection of lines from the input file(s)
        PCollection<String> lines = p.apply(TextIO.read().from("gs://terrys-dataflow-bucket/random-text.txt"));


        output_lines_p_collection(lines);

        manual_word_count(lines);

        streamlines_word_count(lines);

        p.run().waitUntilFinish();
    }

    private static void output_lines_p_collection(PCollection<String> lines) {
        lines.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                if(line.length() > 0) {
                    LOG.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" +
                    "line length = " + line.length() + " \n" +
                    "line = " + line + "\n" +
                    "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                }
            }}));
    }

    private static void manual_word_count(PCollection<String> lines) {

        // Don't need this because TextIO.read() returns a lines PCollection by default
//        PCollection<String> lineCollection =
//            lines.apply(ParDo.of(new DoFn<String, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                String line = c.element();
//                if(line.length() > 0) {
//                    LOG.info("line = " + line);
//                    c.output(line);
//                }
//            }}));

        // https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/ParDo
        PCollection<String> wordCollection =
                lines.apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String line = c.element();
                        for (String word : line.split("[^a-zA-Z']+")) {
                            c.output(word);
                            LOG.info("word = " + word);
                        }
                    }}));

        PCollection<String> filteredWordCollection = wordCollection.apply(Filter.by((String word) -> !word.isEmpty()));

        PCollection<KV<String, Long>> countPerWord = filteredWordCollection.apply(Count.perElement()); //Count.PerElement is an accumulator

        PCollection<String> outputCountPerWord = countPerWord.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {         //Output results as a string

            @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> wordCountKVPair = c.element();
                        String output = wordCountKVPair.getKey() + " " + wordCountKVPair.getValue();
                        LOG.info(output);
                        c.output(output);
                    }}));

        outputCountPerWord.apply(TextIO.write().to("a_wordcounts"));

    }

    private static void streamlines_word_count(PCollection<String> lines) {

        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        lines.apply(FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))

                // We use a Filter transform to avoid empty word
                .apply(Filter.by((String word) -> !word.isEmpty()))

                // Concept #3: Apply the Count transform to our PCollection of individual words. The Count
                // transform returns a new PCollection of key/value pairs, where each key represents a
                // unique word in the text. The associated value is the occurrence count for that word.
                .apply(Count.perElement())

                // Apply a MapElements transform that formats our PCollection of word counts into a
                // printable string, suitable for writing to an output file.
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))

                // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.
                // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of
                // formatted strings) to a series of text files.
                //
                // By default, it will write to a set of files with names like wordcounts-00001-of-00005
                .apply(TextIO.write().to("a_streamlines_word_count"));
    }

}
