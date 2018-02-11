import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer extends App {

  val properties = new Properties()

  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  properties.setProperty("acks", "1")
  properties.setProperty("retries", "3")
  properties.setProperty("linger.ms", "1")

  val producer = new KafkaProducer[String, String](properties)

  0 until 10 foreach { key =>
    val producerRecord = new ProducerRecord[String, String]("second_topic", key.toString, s"message from key: $key")
    producer.send(producerRecord)
  }

  producer.close()

}
