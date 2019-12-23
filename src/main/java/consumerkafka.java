import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * flink读取kafka topic "test-1-topic"的数据，wordcount处理后,写入 kafka topic  "test-2-topic" 中
 */
public class consumerkafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.enableCheckpointing(5000); // checkpoint every 5000 msecs,启用flink检查点之后，flink会定期checkpoint offset，万一作业失败，Flink将把流式程序恢复到最新检查点的状态，并从存储在检查点的偏移量开始重新使用Kafka的记录
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.10.10.16:9092");
        properties.put("metadata.broker.list","10.10.10.16:9092");
      //  properties.setProperty("group.id", "test");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("test-1-topic", new SimpleStringSchema(), properties);
        //指定偏移量
       // myConsumer.setStartFromEarliest();
        myConsumer.setStartFromLatest();

        DataStream<String> stream = env
                .addSource(myConsumer);

        stream.print();
        //定义producer
        FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>("10.10.10.16:9092","test-2-topic",new SimpleStringSchema());
        DataStream<WordWithCount>  wordcount = stream.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word:splits) {
                    collector.collect(new WordWithCount(word,1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2),Time.seconds(1))//指定计算数据的窗口大小和滑动窗口大小
                .sum("count");
        //把数据打印到控制台
        wordcount.print()
                .setParallelism(1);//使用一个并行度

        DataStreamSink<String> wordcountstring = wordcount.map(new MapFunction<WordWithCount, String>() {
            @Override
            public String map(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.toString();
            }
        }).addSink(myProducer).setParallelism(1);

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("Flink Streaming Java API Skeleton");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
