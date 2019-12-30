package com.adc.flinkdemo
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

object KafkaConnectorProducer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    val data = env.socketTextStream("10.10.10.16",9999)

    val topic ="test-2-topic"

    val  properties = new Properties()
    properties.setProperty("bootstrap.servers","10.10.10.16:9092")
    //properties.setProperty("group.id","test")

    //env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties))
    val kafkaSink = new FlinkKafkaProducer09[String](topic,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),properties)
    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducer")


  }

}
