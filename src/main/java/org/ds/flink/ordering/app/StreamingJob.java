

package org.ds.flink.ordering.app;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.ds.flink.ordering.pojos.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class StreamingJob {

	private static Logger log = LoggerFactory.getLogger(StreamingJob.class);
	private static final String region = System.getenv("AWS_REGION");
	private static String inputStreamName = "state-stream";

	private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
		Properties inputProperties = new Properties();
		inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
		inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

		return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties))
				.name("state input")
				.uid("state input");
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		long baselineTimestamp = System.currentTimeMillis();

		DataStream<String> input = createSourceFromStaticConfig(env);

		input
				.flatMap(new MutationFlatmapper())
				.keyBy(m -> m.id)
				.timeWindow(Time.seconds(2))
				.aggregate(new AggregateFunction<Mutation, List<Mutation>, List<Mutation>>() {


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
				})
				.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
