package com.arb.schema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class SchemaDefinitionAndProcessing_v3 {

    private static final String CSV_HEADER = "Date,Product,Card,Country";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from("src/main/resources/source/SalesJan2009.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ParseSalesRecord()))
                .apply(Select.fieldNames("product", "price","date"))
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((Row row) ->
                                row.getString("product") + "," + row.getFloat("price") + ","+ row.getString("date")))
                .apply("WriteResult",
                        TextIO.write()
                                .to("src/main/resources/sink/product_price")
                                .withSuffix(".csv")
                                .withShardNameTemplate("-SSS")
                                .withHeader("Product,Price,Date"));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class ParseSalesRecord extends DoFn<String, SalesRecord_v2> {

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<SalesRecord_v2> out) {
            String[] data = line.split(",");

            SalesRecord_v2 record = new SalesRecord_v2(data[0], data[1],
                    Integer.parseInt(data[2]), data[3], data[4]);

            out.output(record);
        }
    }
}
