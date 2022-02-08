package KafkaStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class CountEmail {

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"id_app_1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder= new StreamsBuilder();
        KStream<String,String> source = builder.stream("source-topic");

        /*source.mapValues(v->v.toUpperCase())
                .flatMapValues(v-> Arrays.asList((v.split(" "))))
                //.selectKey((k,v)->v)
                // .groupByKey()
                .groupBy((key,value)->value)
                //.count(Materialized.with(Serdes.String(),Serdes.Long()))
                .count(Materialized.as("Count-store-isga"))
                .toStream()
                .to("out-source");*/

        KStream<String,String> ks0=source.mapValues(value->value.toUpperCase());
        KStream ks1=ks0.flatMapValues(value-> Arrays.asList((value.split("\t"))));
        ks1.print(Printed.<String,String>toSysOut().withLabel("KS_SPLIT ---- "));
        KStream ks2=ks1.selectKey((key,value)->value);
        //ks2 = ks2.filter((key,value)->value.contains());
        ks2.print(Printed.<String,String>toSysOut().withLabel("KS_NEW_KEY ---- "));
        KGroupedStream<String,String> ks3=ks2.groupByKey();
        KTable<String,Long> kt1=ks3.count(Materialized.with(Serdes.String(),Serdes.Long()));
        kt1.toStream().print(Printed.<String,Long>toSysOut().withLabel("KT_COUNT ---- "));
        System.out.println("----------- AFFICHAGE KSTREAMS ET KTABLE");

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology,properties);
        kafkaStreams.start();

    }
}
