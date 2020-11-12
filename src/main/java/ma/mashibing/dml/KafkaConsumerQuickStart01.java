package ma.mashibing.dml;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaConsumerQuickStart01 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group01");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
       /* props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);*/
        //创建生成者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅相关的topic
        consumer.subscribe(Pattern.compile("^topic.*"));
        //consumer.subscribe(Arrays.asList("topic01"));
        //遍历消息队列
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));
                    consumer.commitSync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            System.out.println("map:" + map);
                        }
                    });
                    System.out.println("topic:" + topic + " partition:" + partition + " offset:" + offset + " key:" + key + " value:" + value);
                }
            }
        }

    }
}
