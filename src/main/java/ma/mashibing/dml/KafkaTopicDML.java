package ma.mashibing.dml;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicDML {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建KafkaAdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC9092");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);
        //创建topic信息
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 3)));
        //有下列编码：同步创建 否则异步
        topics.all().get();
        //查看topic列表
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> names = listTopicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }
        /*//删除topic
        DeleteTopicsResult deletes = adminClient.deleteTopics(Arrays.asList("topic02", "topic03"));
        deletes.all().get();*/
        //查看topic详细信息
        DescribeTopicsResult describeTopics = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> map = describeTopics.all().get();
        for (Map.Entry<String, TopicDescription> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "---" + entry.getValue());
        }

        //2.关闭AdminClient
        adminClient.close();
    }
}
