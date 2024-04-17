package com.arb.schema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;


/**
 * This will fail with Caused by: org.apache.beam.sdk.coders.CannotProvideCoderException: Unable to provide a Coder for com.arb.schema.SalesRecord_v1.
 * As we don't have a schema defined for the Pojo
 *
 */
public class SchemaDefinitionAndProcessing_v2_fixed {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<SalesRecord_v2> salesRecords = readSalesData(pipeline);

        System.out.println("Has schema? " + salesRecords.hasSchema());
        System.out.println("Schema: " + salesRecords.getSchema());

        salesRecords.apply(MapElements.via(new SimpleFunction<SalesRecord_v2, Void>() {
            @Override
            public Void apply (SalesRecord_v2 input){
                System.out.println(input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }

    private static PCollection<SalesRecord_v2> readSalesData(Pipeline pipeline) {

        SalesRecord_v2 record1 = new SalesRecord_v2(
                "1/5/09 5:39", "Shoes",
                120, "Amex", "Netherlands");
        SalesRecord_v2 record2 = new SalesRecord_v2(
                "2/2/09 9:16", "Jeans",
                110, "Mastercard", "United States");
        SalesRecord_v2 record3 = new SalesRecord_v2(
                "3/5/09 10:08", "Pens",
                10, "Visa", "United States");
        SalesRecord_v2 record4 = new SalesRecord_v2(
                "4/2/09 14:18", "Shoes",
                303, "Visa", "United States");
        SalesRecord_v2 record5 = new SalesRecord_v2(
                "5/4/09 1:05", "iPhone",
                1240, "Diners", "Ireland");
        SalesRecord_v2 record6 = new SalesRecord_v2(
                "6/5/09 11:37", "TV",
                1503, "Visa", "Canada");

        return pipeline.apply(Create.of(record1, record2, record3, record4, record5, record6));
    }
}
