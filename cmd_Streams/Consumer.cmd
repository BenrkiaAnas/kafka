c:\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic out-source --property print.key=true --property print.value=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --from-beginning