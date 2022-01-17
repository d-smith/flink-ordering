package org.ds.flink.ordering.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.ds.flink.ordering.pojos.Mutation;

import java.util.List;

public class UnpackMutations implements FlatMapFunction<List<Mutation>, Mutation> {
    @Override
    public void flatMap(List<Mutation> mutationList, Collector<Mutation> collector) throws Exception {
        mutationList.forEach(m -> collector.collect(m));
    }
}
