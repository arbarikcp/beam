package com.arb.apache;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import java.util.Arrays;
import java.util.List;

public class Aggregation {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> googStockPrices = Arrays.asList(1367.36, 1360.66, 1394.20,
                1393.33, 1404.31, 1419.82, 1429.73);

        pipeline.apply(Create.of(googStockPrices))
                .apply(Mean.<Double>globally())
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {

                    @Override
                    public Void apply(Double input) {
                        System.out.println("Average stock price: " + input);
                        return null;
                    }
                }));

        pipeline.run();
    }

}
