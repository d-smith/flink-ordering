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

            int idx = (int) (Math.random() * 5);

            //Events created in the correct time order
            String create = String.format("entity%d,create,%d", idx,System.currentTimeMillis());
            Thread.sleep(10);
            String update1 = String.format("entity%d,update1,%d", idx,System.currentTimeMillis());
            Thread.sleep(10);
            String update2 = String.format("entity%d,update2,%d", idx,System.currentTimeMillis());


            //Events ingested in the wrong order


            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), update1.getBytes(StandardCharsets.UTF_8));
            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), create.getBytes(StandardCharsets.UTF_8));
            streamWriter.writeToStream("state-stream", UUID.randomUUID().toString(), update2.getBytes(StandardCharsets.UTF_8));

            Thread.sleep(1000);
        }
    }
}

