object Consumer extends App {

  // Step 0. Imports for Scala API for Kafka Streams
  import org.apache.kafka.streams.scala._
  import ImplicitConversions._
  import Serdes._

  // Step 1. Describe Topology
  // Consume records from input topic and produce records to upper topic
  val builder = new StreamsBuilder

  val uppers = builder
    .stream[String, String]("JavaScalaTopic") // Consume records as a stream
    .mapValues(_.toUpperCase)        // Transform (map) the values


  // Produce records to "upper" topic
  uppers.to("result")

  // Print out records to stdout for debugging purposes
  import org.apache.kafka.streams.kstream.Printed
  val sysout = Printed
    .toSysOut[String, String]
    .withLabel("stdout")
  uppers.print(sysout)

  // Step 2. Build Topology
  val topology = builder.build

  // You can describe the topology and just finish
  println(topology.describe())

  // That finishes the "declaration" part of developing a Kafka Stream application
  // Nothing is executed at this time (no threads have started yet)

  // Step 3. Specify Configuration
  import java.util.Properties
  val props = new Properties()
  import org.apache.kafka.streams.StreamsConfig
  val appId = this.getClass.getSimpleName.replace("$", "")
  println(appId)
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
  props.put(StreamsConfig.CLIENT_ID_CONFIG, appId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")

  // Step 4. Create Kafka Streams Client
  import org.apache.kafka.streams.KafkaStreams
  val ks = new KafkaStreams(topology, props)

  // Step 5. Start Stream Processing, i.e. consuming, processing and producing records
  ks.start

}