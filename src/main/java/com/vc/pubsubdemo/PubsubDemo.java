package com.vc.pubsubdemo;

import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

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

        String projectId = args[0].split("=")[1];

        String topicName = TOPIC_NAME_TEMPLATE.replace("PROJECT_NAME", projectId)
                .replace("TOPIC_NAME", args[1].split("=")[1]);
        String bucketName = args[2].split("=")[1];

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        myOptions.setStreaming(true);
        myOptions.setJobName("Pubsub-stream-0");

//        // Window Interval & Frequency
//        Duration interval = Duration.millis(1000);
//        Duration frequency = interval.dividedBy(2);


        CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(projectId)
                .withInstanceId("vc-big-table-demo")
                .withTableId("vc-bigtable-demo")
                .build();


        Pipeline pipeline = Pipeline.create(myOptions);

        PCollectionView<List<String>> suppData = pipeline.apply("Read Supplement Data", TextIO.read().from("gs://" + bucketName + "/supplement_data.txt"))
                .apply(View.<String>asList());


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


        /**
         *  Manipulate JOINT PCollection
         */
        PCollection<String> jointPCollection = PCollectionList
                .of(branch1)
                .and(branch2)
                .apply(Flatten.pCollections());

        /**
         * Write the UNION list to BigTable
         */
        jointPCollection.apply(ParDo.of(new DoFn<String, Mutation>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
                c.output(new Put(c.element().getBytes())
                        .addColumn("vcbt".getBytes(),
                                    UUID.randomUUID().toString().getBytes(),
                                    c.element().getBytes())
                );
            }
        })).apply(CloudBigtableIO.writeToTable(config));


        // Using Flatten to merge PCollectionsView (Supplement data)
        PCollection<String> finalResult = jointPCollection
                .apply("PrintAllRecords", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        List<String> sideInput = c.sideInput(suppData);

                        StringBuffer sb = new StringBuffer();

                        sideInput.forEach(record -> sb.append(record).append("\r\n"));

                        c.output(c.element());
                        c.output(sb.toString());

                        logger.info("Final Records : " + c.element());
                    }
                }).withSideInputs(suppData));


        //Write result to GCS
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

