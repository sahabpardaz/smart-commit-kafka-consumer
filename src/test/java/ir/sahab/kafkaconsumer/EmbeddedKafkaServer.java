package ir.sahab.kafkaconsumer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An embedded Kafka server which is provided to use in unit tests.
 */
public class EmbeddedKafkaServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
    private static String localIp;

    private KafkaServerStartable broker;

    private EmbeddedZkServer zkServer;
    private Properties kafkaBrokerConfig = new Properties();
    private File logDir;
    private int brokerPort;

    static {
        // Kafka does not work well with 127.0.0.1 or localhost. See here for example:
        // https://www.ibm.com/support/knowledgecenter/SSPT3X_4.1.0/
        // com.ibm.swg.im.infosphere.biginsights.trb.doc/doc/trb_kafka_producer_localhost.html
        // So we are going to use the local ip address instead of localhost.
        try {
            localIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new AssertionError("Failed to get local host address", e);
        }
    }

    public void start() throws IOException, InterruptedException {
        this.zkServer = new EmbeddedZkServer();
        this.zkServer.start();
        logDir = Files.createTempDirectory("kafka").toFile();
        brokerPort = anOpenPort();

        // Configs 'host.name' and 'advertised.host.name' are deprecated since kafka version 1.1.
        // Use 'listeners' and 'advertised.listeners' instead of them. See this:
        // https://kafka.apache.org/11/documentation.html#configuration
        kafkaBrokerConfig.setProperty(KafkaConfig.ZkConnectProp(), zkServer.getAddress());
        kafkaBrokerConfig.setProperty(KafkaConfig.BrokerIdProp(), "1");
        kafkaBrokerConfig.setProperty(KafkaConfig.ListenersProp(),
                String.format("PLAINTEXT://%s:%s", localIp, brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.AdvertisedListenersProp(),
                String.format("PLAINTEXT://%s:%s", localIp, brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.PortProp(), Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        kafkaBrokerConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "true");
        kafkaBrokerConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        this.broker = new KafkaServerStartable(new KafkaConfig(this.kafkaBrokerConfig));
        this.broker.startup();
    }

    public void createTopic(String topicName, Integer numPartitions) {
        ZkClient zkClient = null;
        ZkUtils zkUtils;
        try {
            // When not initializing the ZkClient with ZKStringSerializer, createTopic will return without error.
            // The topic will exist in zookeeper and be returned when listing topics, but Kafka itself does
            // not create the topic.
            zkClient = new ZkClient(zkServer.getAddress(), 30000, 30000, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zkServer.getAddress()), JaasUtils.isZkSecurityEnabled());
            logger.info("Executing create Topic: " + topicName + ", partitions: " + numPartitions
                    + ", replication-factor: 1.");
            AdminUtils.createTopic(zkUtils, topicName, numPartitions, 1, new Properties(), Disabled$.MODULE$);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public String getBrokerAddress() { return localIp + ":" + brokerPort; }

    @Override
    public void close() throws IOException {
        if(this.broker != null)
            this.broker.shutdown();

        if(this.zkServer != null)
            this.zkServer.close();

        if (logDir != null)
            FileUtils.deleteDirectory(logDir);
    }

    private static Integer anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }
}

