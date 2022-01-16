package org.ds.flink.ordering.windows;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.ds.flink.ordering.pojos.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class WindowSorter extends ProcessWindowFunction<Mutation, Mutation, String, TimeWindow> {
    private static Logger log = LoggerFactory.getLogger(WindowSorter.class);
    @Override
    public void process(String key, Context context, Iterable<Mutation> iterable, Collector<Mutation> collector) throws Exception {
        log.info("window process called for key {}", key);

        List<Mutation> ml = new ArrayList<>();
        iterable.forEach(m -> ml.add(m));

        Collections.sort(ml, new Comparator<Mutation>() {
            @Override
            public int compare(Mutation o1, Mutation o2) {
                return o1.timeStamp.compareTo(o2.timeStamp);
            }
        });

        ml.forEach(m -> collector.collect(m));
    }
}
