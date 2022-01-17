

package org.ds.flink.ordering.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.ds.flink.ordering.pojos.Mutation;
import org.ds.flink.ordering.windows.WindowSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;


public class StreamingJob2 {

	private static Logger log = LoggerFactory.getLogger(StreamingJob2.class);
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

	// this drove home the event time and timestamps are really about assigning events to the
	// correct time window, not ordering the processing
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		long baselineTimestamp = System.currentTimeMillis();

		DataStream<String> input = createSourceFromStaticConfig(env);


		input
				.flatMap(new MutationFlatmapper())

				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<Mutation>forBoundedOutOfOrderness(Duration.ofSeconds(1))
						.withTimestampAssigner((event,timestamp)->{
							//log.info("ts {}", event.timeStamp);
							return event.timeStamp;
						})
				)
				.keyBy(mutation -> mutation.id)
				//.timeWindow(Time.seconds(5))
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.process(new WindowSorter())
				.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
