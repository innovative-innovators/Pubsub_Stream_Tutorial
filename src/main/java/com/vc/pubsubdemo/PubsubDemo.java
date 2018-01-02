package com.vc.pubsubdemo;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Vincent on 2017/12/30.
 */
public class PubsubDemo {

    public static String TOPIC_NAME_TEMPLATE = "projects/PROJECT_NAME/topics/TOPIC_NAME";
    private static Logger logger = LoggerFactory.getLogger(PubsubDemo.class);

    public interface MyOptions extends DataflowPipelineOptions {
        @Description("Over how long a time period should we average? (in minutes)")
        @Default.Double(60.0)
        Double getAveragingInterval();

        void setAveragingInterval(Double d);

        String getTopic();
        void setTopic(String topic);

        String getBucket();
        void setBucket(String bucket);
    }

    public static void main(String args[]) {

        String topicName = TOPIC_NAME_TEMPLATE.replace("PROJECT_NAME", args[0].split("=")[1])
                .replace("TOPIC_NAME", args[1].split("=")[1]);
        String bucketName = args[2].split("=")[1];

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        myOptions.setStreaming(true);
        myOptions.setJobName("Pubsub-stream-0");

//        // Window Interval & Frequency
//        Duration interval = Duration.millis(1000);
//        Duration frequency = interval.dividedBy(2);

        Pipeline pipeline = Pipeline.create(myOptions);

        PCollection<String> suppData = pipeline.apply("Read Supplement Data", TextIO.read().from("gs://vc-bucket/TrainData_Fraud_gcpml.csv"));

        PCollection<String> input = pipeline
                .apply("ReceiveMessage", PubsubIO.readStrings().fromTopic(topicName))
                .apply("TimeWindow",
                        Window.<String>into(FixedWindows.of(Duration.millis(500)))
                                .triggering(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.millis(500))
                                ).withAllowedLateness(Duration.millis(100))
                                .accumulatingFiredPanes()
                )
                .apply("PrintMessage", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        logger.info("Received message is :" + c.element());
                        c.output("Received message is :" + c.element()); // Deliver propagation
                    }
                }));


        /**
         * For splitting different branches
         */

        PCollection<String> branch1 = input.apply("ManipulateOne", ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        logger.info("ManipulateOne :" + c.element() + "------One!");
                        c.output("ManipulateOne :" + c.element() + "------One!"); // Deliver propagation
                    }
                }
        ));

        PCollection<String> branch2 = input.apply("ManipulateTwo", ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        logger.info("ManipulateTwo :" + c.element() + "------Two!");
                        c.output("ManipulateTwo :" + c.element() + "------Two!"); // Deliver propagation
                    }
                }
        ));


        // Using Flatten to merge PCollections
        PCollection<String> finalResult = PCollectionList
                .of(branch1)
                .and(branch2)
                .apply(Flatten.pCollections())
                .apply("PrintAllRecords", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element());
                        logger.info("Final Records : " + c.element());
                    }
                }));


        //Write result to GCS
        // finalResult.apply(TextIO.write().withWindowedWrites().withNumShards(1).to("gs://vc-bucket/pubsub-final-result/final-result").withSuffix("txt"));
        finalResult.apply(
                TextIO.write()
                        .withoutSharding()
                        .withWindowedWrites()
                        .to("gs://" + bucketName + "/pubsub-final-result/final-result-" +
                                new SimpleDateFormat("YYYYMMdd_HHmmss").format(Calendar.getInstance().getTime())
                        )
                        .withSuffix(".txt")
        );

        pipeline.run();

    }

}

