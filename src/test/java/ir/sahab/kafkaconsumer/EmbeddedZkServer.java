package ir.sahab.kafkaconsumer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * An embedded ZK server which is provided to use in unit tests.
 */
public class EmbeddedZkServer implements Closeable {
    private String localIp;

    private ServerCnxnFactory factory;
    private File snapshotDir;
    private File logDir;
    private int port;

    /**
     * Starts an embedded zk server. It should be called just once. But you can call
     * {@link #stop()} and {@link #restart()} multiple times.
     */
    public void start() throws IOException, InterruptedException {
        snapshotDir = Files.createTempDirectory("zk-snapshot").toFile();
        logDir = Files.createTempDirectory("zk-logs").toFile();
        // Why we are going to use local ip and not just localhost or 127.0.0.1 constants?
        // It is because EmbeddedKafkaServer (one of the user of this class), does not works well
        // with localhost.
        // See here for example:
        // https://www.ibm.com/support/knowledgecenter/SSPT3X_4.1.0/
        // com.ibm.swg.im.infosphere.biginsights.trb.doc/doc/trb_kafka_producer_localhost.html
        // So we are going to use the local ip address instead of localhost
        localIp = InetAddress.getLocalHost().getHostAddress();
        this.port = anOpenPort();
        restart();
    }

    /**
     * Restarts the zk server. It should be called after a call to {@link #stop()}.
     * After restart, the data you have added to Zk before {@link #stop()} is still there.
     */
    public void restart() throws IOException, InterruptedException {
        // ZooKeeperServer overrides DefaultUncaughtExceptionHandler, but in our unit tests,
        // we have set a custom DefaultUncaughtExceptionHandler which assert on any unhandled
        // errors and we do not want anyone to override this behaviour.
        // So here, we are going to backup the DefaultUncaughtExceptionHandler before
        // creating ZkServer and restore it after.
        Thread.UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();

        ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, 500 /*tick time*/);
        this.factory = NIOServerCnxnFactory.createFactory();
        this.factory.configure(new InetSocketAddress(localIp, port), 100 /*Max clients*/);
        this.factory.startup(zkServer);

        // Restore the DefaultUncaughtExceptionHandler.
        Thread.setDefaultUncaughtExceptionHandler(handler);
    }

    @Override
    public void close() throws IOException {
        stop();
        if (logDir != null)
            FileUtils.deleteDirectory(logDir);
        if (snapshotDir != null)
            FileUtils.deleteDirectory(snapshotDir);
    }

    /**
     * Stops the zk server but do not releases all resources. If you want to release all resources,
     * you should call {@link #close()}.
     */
    public void stop() {
        if (factory != null)
            this.factory.shutdown();
    }

    public String getAddress() {
        return localIp + ":" + port;
    }

    public int getPort() {
        return port;
    }

    private static Integer anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }
}

