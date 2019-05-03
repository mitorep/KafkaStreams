#input topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic JavaScalaTopic --from-beginning
#output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic result --from-beginning

