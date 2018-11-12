package ir.sahab.kafkaconsumer;

/**
 * Holder of both partition and offset of a Kafka record.
 */
public class PartitionOffset {
    private int partition;
    private long offset;

    public PartitionOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }
}
