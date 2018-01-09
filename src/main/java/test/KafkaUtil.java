package test;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhoucw on 下午6:10.
 */
public class KafkaUtil {

    public void getTopic() {
        Map<String, List<PartitionInfo>> topics;

        Properties props = new Properties();
        props.put("bootstrap.servers", "1.2.3.4:9092");
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        ZkConnection zkConnection = new ZkConnection("");


    }

    public static void main(String[] args) {

    }
}
