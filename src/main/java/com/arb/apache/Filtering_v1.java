package com.arb.apache;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class Filtering_v1 {

    public static class FilterThresholdFn extends DoFn<Double, Double> {
        private double threshold = 0;

        public FilterThresholdFn(double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() > threshold) {
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> googStockPrices = Arrays.asList(1367.36, 1360.66, 1394.20,
                1393.33, 1404.31, 1419.82, 1429.73);

        PCollection<Double> stPrices =  pipeline.apply(Create.of(googStockPrices));
        pipeline.apply(Create.of(googStockPrices))
                .apply(ParDo.of(new FilterThresholdFn(1400)));

        pipeline.run();
    }

}