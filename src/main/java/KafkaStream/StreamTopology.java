package KafkaStream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class StreamTopology {
    public static void main(String[] args) {
        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,String> source=builder.stream(("src-topic"));
        KStream<String,String> UpperSource=source.mapValues(value->value.toUpperCase());
        UpperSource.to("out-topic");

        Topology topology=builder.build();
        System.out.println(topology.describe());
    }
}
