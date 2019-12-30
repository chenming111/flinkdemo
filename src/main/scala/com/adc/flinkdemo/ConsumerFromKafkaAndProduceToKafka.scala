package com.adc.flinkdemo

import java.util.Properties

import com.adc.flinkdemo.SocketWindowWordCountScala.WordWithCount
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 从kafka消费数据，经过flink处理后发送到另一个kafka topic
 */
object ConsumerFromKafkaAndProduceToKafka {
  //定义常量
  private val KAFKA_BROKER = "10.10.10.16:9092"
  private val KAFKA_CONSUMER_TOPIC ="test-1-topic"
  private val KAFKA_PRODUCER_TOPIC ="test-2-topic"
  // 定义一个数据类型保存单词出现的次数
  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
   // env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = new Properties()
     kafkaProps.setProperty("bootstrap.servers",KAFKA_BROKER)


    val text: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](KAFKA_CONSUMER_TOPIC, new SimpleStringSchema(), kafkaProps)
      .setStartFromLatest())

      text.print()
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(2))
      .sum("count")
      .map{w => w.toString}

     windowCounts.print().setParallelism(1)

    val result = windowCounts.addSink(new FlinkKafkaProducer09[String](KAFKA_BROKER,KAFKA_PRODUCER_TOPIC,new SimpleStringSchema()))
        .setParallelism(1)

    env.execute("ConsumerFromKafkaAndProduceToKafka")

  }

}
