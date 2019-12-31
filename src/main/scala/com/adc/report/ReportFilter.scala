package com.adc.report

import java.util.Properties

import com.adc.flinkdemo.ConsumerFromKafkaAndProduceToKafka.{KAFKA_BROKER, KAFKA_CONSUMER_TOPIC, KAFKA_PRODUCER_TOPIC}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

/**
 * scala版本消费kafka数据，自定义MapFunction处理后，发到另一个topic的kafka中
 */
object ReportFilter {
  //定义常量
  private val KAFKA_BROKER = "10.10.10.16:9092"
  private val KAFKA_CONSUMER_TOPIC ="test-1-topic"
  private val KAFKA_PRODUCER_TOPIC ="test-2-topic"
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers",KAFKA_BROKER)
    val reportinformation: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](KAFKA_CONSUMER_TOPIC, new SimpleStringSchema(), kafkaProps)
      .setStartFromLatest())
    reportinformation.print().setParallelism(1)
    val filterinformation: DataStream[String]= reportinformation.map(new MapFunction[String,String] {
      override def map(t: String): String = {
        val strarray=t.split("\t")
        for (i <- 0 until strarray.length) {

          if (strarray(i).contains(":")){
            strarray(i)=strarray(i).substring(strarray(i).indexOf(":")+1,strarray(i).length)
          }
        }
        strarray.mkString("\t")
      }
    })

    filterinformation.addSink(new FlinkKafkaProducer09[String](KAFKA_BROKER,KAFKA_PRODUCER_TOPIC,new SimpleStringSchema())).setParallelism(1)
    filterinformation.print()

    env.execute("ReportFilter")

  }



}
