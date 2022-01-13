package org.ds.flink.ordering.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.ds.flink.ordering.pojos.Mutation;

public class MutationFlatmapper implements FlatMapFunction<String, Mutation> {
    @Override
    public void flatMap(String s, Collector<Mutation> collector) throws Exception {
        try {
            String[] tokens = s.split(",");
            Long.valueOf(tokens[2]);
            if(tokens.length == 3) {
                collector.collect(
                        new Mutation(tokens[0], tokens[1], Long.valueOf(tokens[2]))
                );
            }
        } catch(Throwable t) {
            return;
        }

    }
}
