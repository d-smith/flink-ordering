package org.ds.flink.ordering.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.ds.flink.ordering.pojos.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AggregateSorter implements AggregateFunction<Mutation, List<Mutation>, List<Mutation>> {

    private static Logger log = LoggerFactory.getLogger(AggregateSorter.class);

    @Override
    public List<Mutation> createAccumulator() {
        log.info("create acc");
        return new ArrayList<>();
    }

    @Override
    public List<Mutation> add(Mutation mutation, List<Mutation> mutations) {
        log.info("add");
        mutations.add(mutation);
        return mutations;
    }

    @Override
    public List<Mutation> getResult(List<Mutation> mutations) {
        log.info("get result");
        Collections.sort(mutations, new Comparator<Mutation>() {
            @Override
            public int compare(Mutation o1, Mutation o2) {
                return o1.timeStamp.compareTo(o2.timeStamp);
            }
        });

        return mutations;
    }

    @Override
    public List<Mutation> merge(List<Mutation> mutations, List<Mutation> acc1) {
        log.info("merge");
        mutations.addAll(acc1);
        return mutations;
    }
}
