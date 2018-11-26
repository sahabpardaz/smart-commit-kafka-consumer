package ir.sahab.kafkaconsumer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
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
        logDir = Files.createTempDirectory("kafka", new FileAttribute[0]).toFile();
        brokerPort = anOpenPort();

        this.kafkaBrokerConfig.setProperty("zookeeper.connect", zkServer.getAddress());
        this.kafkaBrokerConfig.setProperty("broker.id", "1");
        this.kafkaBrokerConfig.setProperty("host.name", localIp);
        this.kafkaBrokerConfig.setProperty("advertised.host.name", localIp);
        this.kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        this.kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        this.kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        this.kafkaBrokerConfig.setProperty("auto.create.topics.enable", "true");
        this.kafkaBrokerConfig.setProperty("offsets.topic.replication.factor", "1");
        this.broker = new KafkaServerStartable(new KafkaConfig(this.kafkaBrokerConfig));
        this.broker.startup();
    }

    public void createTopic(String topicName, Integer numPartitions) {
        String[] arguments = new String[]{"--create", "--zookeeper", zkServer.getAddress(),
                "--replication-factor", "1",
                "--partitions", "" + numPartitions,
                "--topic", topicName};
        TopicCommandOptions opts = new TopicCommandOptions(arguments);
        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        logger.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.createTopic(zkUtils, opts);
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

