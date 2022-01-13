package org.ds.flink.ordering.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class QuotesProducer {
    static Logger LOG = LoggerFactory.getLogger(QuotesProducer.class);

    public static void main(String... args) throws Exception {

        StreamWriter streamWriter = new StreamWriter();

        for (; ; ) {

            int idx = (int) (Math.random() * 10);

            long timestamp = System.currentTimeMillis();

            String update1 = String.format("entity%d,update1,%d", idx,timestamp +  1000);
            String create = String.format("entity%d,create,%d", idx,timestamp);
            String update2 = String.format("entity%d,update2,%d", idx,timestamp + 2000);

            //LOG.info("quoting {}", quoteString);
            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), update1.getBytes(StandardCharsets.UTF_8));
            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), create.getBytes(StandardCharsets.UTF_8));
            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), update2.getBytes(StandardCharsets.UTF_8));
            Thread.sleep(10000);
        }
    }
}

