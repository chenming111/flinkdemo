import org.apache.commons.httpclient.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.Properties;

/**
 * 对从kafka一个topic消费的报告信息进行过滤处理，发送到另一个topic
 */
public class ReportFilter {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
       // env.enableCheckpointing(5000); // checkpoint every 5000 msecs,启用flink检查点之后，flink会定期checkpoint offset，万一作业失败，Flink将把流式程序恢复到最新检查点的状态，并从存储在检查点的偏移量开始重新使用Kafka的记录
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
        DataStream<String> stream = env.addSource(myConsumer);
        stream.print().setParallelism(1);
        DataStream<String> filterinformation= stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] strarray= s.split("\t");
                StringBuffer sb = new StringBuffer();
                for (String str:strarray){
                    if (str.contains(":")){
                        str=str.substring(str.indexOf(":")+1,str.length());
                    }
                    sb.append(str).append("\t");
                }
             return sb.toString();
            }
        });

        /*DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
                .create()
                .withMaxPartSize(1024*1024*120) // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
                .withRolloverInterval(Long.MAX_VALUE) // 滚动写入新文件的时间，默认60s。这里设置为无限大
                .withInactivityInterval(60*1000) // 60s空闲，就滚动写入新的文件
                .build();
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://10.10.10.16:8020/test_origin_data/report"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new ReportBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000L) // 桶检查间隔，这里设置为1s
                .build();*/



       /* final StreamingFileSink<String> sink = StreamingFileSink
               .forRowFormat(new Path("hdfs://10.10.10.16:8020/test_origin_data/report"), new SimpleStringEncoder<String>("UTF-8"))
                .build();

        filterinformation.addSink(sink);*/


       //  filterinformation.writeAsText("C:\\Users\\chenming\\Desktop\\"+new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        filterinformation.addSink(new FlinkKafkaProducer09<String>("10.10.10.16:9092","test-2-topic",new SimpleStringSchema()));
        filterinformation.print().setParallelism(1);

        env.execute("ReportFilter");
    }


    private static class ReportBucketAssigner implements org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner<String, String> {
        @Override
        public String getBucketId(String s, Context context) {
            // 指定桶名 yyyy-mm-dd
            String[] array = s.split("\t");
            System.out.println(DateUtil.formatDate(new Date(Long.valueOf(array[5])), "yyyy-MM-dd"));

            return DateUtil.formatDate(new Date(Long.valueOf(array[5])), "yyyy-MM-dd");

        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;

        }
    }
}
