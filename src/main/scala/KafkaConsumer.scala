import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val properties = new Properties()

  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  properties.setProperty("group.id", "test")
  properties.setProperty("enable.auto.commit", "false")
  //  properties.setProperty("enable.auto.commit", "true")
  //  properties.setProperty("auto.commit.interval.ms", "1000")
  properties.setProperty("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](properties)

  consumer.subscribe(Collections.singletonList("second_topic"))

  while (true) {
    val consumerRecords = consumer.poll(100).asScala
    consumerRecords foreach { record =>
      println(s"Partition: ${record.partition()}, " +
        s"Offset: ${record.offset()}, " +
        s"Key: ${record.key()}, " +
        s"Value: ${record.value()}")

    }
    consumer.commitSync()
  }

  consumer.close()

}
