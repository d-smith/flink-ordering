

package org.ds.flink.ordering;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		long baselineTimestamp = System.currentTimeMillis();

		DataStream<Tuple3<String,String,Long>> inputStream = env.fromElements(
				Tuple3.of("entity1","update", baselineTimestamp + 1000),
				Tuple3.of("entity1","create", baselineTimestamp),
				Tuple3.of("entity1","update", baselineTimestamp + 2000)
		);

		inputStream
				.keyBy(key -> key.f0)
				.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
