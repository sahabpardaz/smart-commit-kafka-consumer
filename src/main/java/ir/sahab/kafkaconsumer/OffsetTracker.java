package ir.sahab.kafkaconsumer;

import ir.sahab.logthrottle.LogThrottle;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker for Kafka consumed records which are delivered to their targets. Here is the problem
 * it wants to solve:
 * <p>
 * If we want to have 'at least once' delivery, we should commit an offset in a partition just
 * after making sure that all previous offsets of that partition are delivered.
 * But mostly, we get delivery callbacks of records separately, and not necessarily in order.
 * It is possible to wait for delivery of all records, and then commit the last consumed offsets
 * but meanwhile we can not poll more records because it changes the last offsets.
 * If the waiting time is low, this solution is completely reasonable, because although we
 * do not poll, the underlying receiver thread of {@link KafkaConsumer} is working and buffers
 * new records. But when the delivery time is varying, waiting for all records may reduce our
 * performance if the {@link KafkaConsumer}'s underlying buffer becomes full.
 * Offset tracker helps us to both poll continuously and commit reliably because it tells
 * us which offsets are safe to commit.
 * <p>
 * It is how you should use it:
 * <p>
 * After each poll(), you should call {@link #track(int, long)} for every consumed records to inform
 * tracker of new records. And when you make sure that a record is delivered to its destination,
 * call {@link #ack(int, long)} for that record. Then if there is an offset on that partition which
 * is safe to commit, you get the offset as return value. Note that this class is not thread safe
 * and you should do all track and ack calls from a single thread, which can be your polling thread.
 * For this reason, you can keep delivered records in a queue to drain them from poll loop.
 * The only method which is safe to call from other threads is {@link #reset()}.
 */
public class OffsetTracker {
    private static final Logger logger = LoggerFactory.getLogger(OffsetTracker.class);
    private static final LogThrottle logThrottle = new LogThrottle(logger);

    private final int pageSize;
    private final int maxOpenPagesPerPartition;
    private volatile Map<Integer /*partition*/, PartitionTracker> partitionTrackers;

    /**
     * Constructs a new offset tracker with the specified page size and maximum number of pages.
     * @param pageSize the size of each page. Offsets of each partition are tracked in several
     *                 pages, each of them responsible for a specific range of offsets.
     *                 When all offsets of a page is acked, you get the page tail offset as
     *                 {@link #ack(int, long)}} return value and it is a safe offset to commit.
     * @param maxOpenPagesPerPartition maximum number of open pages (pages which have tracked but
     *                                 not-acked offsets). After reaching to this limit on a
     *                                 partition, next calls to {@link #ack(int, long)} on that
     *                                 partition fails and returns false.
     */
    public OffsetTracker(int pageSize, int maxOpenPagesPerPartition) {
        this.pageSize = pageSize;
        this.maxOpenPagesPerPartition = maxOpenPagesPerPartition;
        this.partitionTrackers = new HashMap<>();
    }

    /**
     * Clears all previous tracks. It is applicable when Kafka consumer is closed or on re-balance.
     * It is safe to call it from a separate thread then the one you call {@link #ack(int, long)}
     * and {@link #track(int, long)} from.
     */
    public void reset() {
        partitionTrackers = new HashMap<>();
    }

    /**
     * Tells the tracker to track specified offset of the given partition.
     * @param partition index of partition that the offset belongs to.
     * @param offset offset which should be tracked.
     * @return true if it is successful and false if the tracker is full (i.e., the maximum number
     *         of open pages is reached on this partition).
     */
    public boolean track(int partition, long offset) {
        PartitionTracker partitionTracker = partitionTrackers.get(partition);
        if (partitionTracker == null) {
            partitionTracker = new PartitionTracker(offset);
            partitionTrackers.put(partition, partitionTracker);
        }
        return partitionTracker.track(offset);
    }

    /**
     * Tells the tracker that specified offset of the given partition is delivered to its target.
     * @param partition index of partition that the offset belongs to.
     * @param offset offset which is acked.
     * @return empty if there is no new offset which is safe to commit on this partition or the
     *         safe offset for commit if exists.
     */
    public OptionalLong ack(int partition, long offset) {
        PartitionTracker partitionTracker = partitionTrackers.get(partition);
        if (partitionTracker == null) {
            logThrottle.logger("missed-ack").warn("An ack received but this offset is not under "
                    + "track. It is valid if it has been a re-balance recently. partition: "
                    + "{}, offset: {}", partition, offset);
            return OptionalLong.empty();
        }
        return partitionTracker.ack(offset);
    }

    /**
     * Tracker of a single partition. {@link OffsetTracker} makes one instance of this class
     * for each partition which is under track.
     */
    private class PartitionTracker {
        private final Map<Long /*page index*/, PageTracker> pageTrackers = new HashMap<>();
        private final SortedSet<Long /*page index*/> completedPages = new TreeSet<>();
        private long lastConsecutivePageIndex;

        public PartitionTracker(long initialOffset) {
            lastConsecutivePageIndex = offsetToPage(initialOffset) - 1;
        }

        boolean track(long offset) {
            long pageIndex = offsetToPage(offset);
            int pageOffset = (int) (offset % pageSize);
            PageTracker pageTracker = pageTrackers.get(pageIndex);
            if (pageTracker == null) {
                if (pageTrackers.size() >= maxOpenPagesPerPartition) {
                    return false;
                }
                pageTracker = new PageTracker(pageSize, pageOffset);
                pageTrackers.put(pageIndex, pageTracker);
                // Since all the offsets will be tracked in order, we don't expect any more tracks for previous page.
                markPageNoMoreTracks(pageIndex - 1);
            }
            pageTracker.track(pageOffset);
            return true;
        }

        /**
         * Marks a page with the noMoreTracks flag and handles necessary stuff if it gets completed.
         */
        private void markPageNoMoreTracks(long pageIndex) {
            PageTracker pageTracker = pageTrackers.get(pageIndex);
            if (pageTracker != null) {
                pageTracker.markNoMoreTracks();
                if (pageTracker.isCompleted()) {
                    pageTrackers.remove(pageIndex);
                    if (pageIndex > lastConsecutivePageIndex) {
                        completedPages.add(pageIndex);
                    }
                }
            }
        }

        /**
         * Tells that specified offset of the partition which this object is responsible for,
         * is delivered to its target.
         * @param offset offset which is acked.
         * @return empty if there is no new offset which is safe to commit on this partition or the
         *         safe offset for commit if exists.
         */
        OptionalLong ack(long offset) {
            // Tell the corresponding page tracker that this offset is acked.
            long pageIndex = offsetToPage(offset);
            PageTracker pageTracker = pageTrackers.get(pageIndex);
            if (pageTracker == null) {
                logThrottle.logger("missed-ack").warn("An ack received but this offset is not under "
                        + "track. It is valid if it has been a re-balance recently. "
                        + "offset: {}", offset);
                return OptionalLong.empty();
            }
            int pageOffset = (int) (offset % pageSize);
            if (!pageTracker.ack(pageOffset)) {
                return OptionalLong.empty();
            }

            // If the page is completed (all offsets in the page is acked), add the pages to the
            // list of completed pages.
            pageTrackers.remove(pageIndex);
            if (pageIndex <= lastConsecutivePageIndex) {
                logThrottle.logger("redundant-page").warn("An ack received which completes a page "
                                + "but the page is already completed. It is valid if it has "
                                + "been a re-balance recently. offset: {}, completed page index: {}",
                        offset, pageIndex);
                return OptionalLong.empty();
            }
            completedPages.add(pageIndex);

            // See whether the completed pages, construct a consecutive chain.
            int numConsecutive = 0;
            Iterator<Long> iterator = completedPages.iterator();
            while (iterator.hasNext()) {
                long index = iterator.next();
                if (index != lastConsecutivePageIndex + 1) {
                    break;
                }
                numConsecutive++;
                lastConsecutivePageIndex = index;
            }

            // There is no consecutive completed pages. So there is no offset to report as
            // safe to commit.
            if (numConsecutive == 0) {
                return OptionalLong.empty();
            }

            // There are consecutive completed pages which are not reported.
            // Remove them and report the next offset for commit.
            iterator = completedPages.iterator();
            for (int i = 0; i < numConsecutive; i++) {
                iterator.next();
                iterator.remove();
            }
            return OptionalLong.of(pageToFirstOffset(lastConsecutivePageIndex + 1));
        }

        private long pageToFirstOffset(long pageIndex) {
            return pageIndex * pageSize;
        }

        private long offsetToPage(long offset) {
            return offset / pageSize;
        }
    }

    /**
     * Tracker of a single page in a partition. {@link PartitionTracker} makes one instance of this
     * class for each page when it starts tracking the first offset of that page.
     */
    private class PageTracker {
        private final int effectiveSize;
        private final int margin;
        private final BitSet bits;
        private int maxTrackedOffset;
        private boolean noMoreTracks = false;

        /**
         * Constructs a page tracker.
         * @param size the page size (number of offsets in the page).
         * @param margin indicates the minimum offset which we expect to get its ack. If page
         *               tracker gets acks of all offsets (from margin to the tail of the page),
         *               it reports that the page is completed.
         */
        PageTracker(int size, int margin) {
            this.effectiveSize = size - margin;
            this.margin = margin;
            this.maxTrackedOffset = 0;
            bits = new BitSet(effectiveSize);
        }

        int getMargin() {
            return margin;
        }

        /**
         * Tracks an offset within this page. All the tracked offsets should be acked before this page can be completed.
         * @param offset pageOffset to track
         */
        void track(int offset) {
            if (offset < margin) {
                throw new IllegalStateException("Out of order offset detected. Consumer offsets should be tracked in order.");
            }
            int effectiveOffset = offset - margin;
            // Set the bit representing this offset, indicating the offset is tracked but not acked yet
            bits.set(effectiveOffset);
            if (effectiveOffset > this.maxTrackedOffset) {
                this.maxTrackedOffset = effectiveOffset;
            }
        }

        /**
         * Tells that specified offset of the page which this object is responsible for,
         * is delivered to its target.
         * @param offset offset which is acked.
         * @return true if the page is completely acked (i.e., all expected offsets of the page
         *         is acked).
         */
        boolean ack(int offset) {
            if (offset < margin) {
                logThrottle.logger("not-tracked-region").warn("An ack received but this offset is "
                        + "not in the tracked region of the page. It is valid if it has been a "
                        + "re-balance recently. offset: {}, page margin: {}", offset, margin);
                return false;
            }

            // Clear the bit representing this offset.
            int effectiveOffset = offset - margin;
            bits.clear(effectiveOffset);

            // Return true if all expected offsets are acked.
            return isCompleted();
        }

        /**
         * Calling this method indicates that this PageTracker should not expect any more new offset tracks and any
         * missing untracked offset is a gap. We may have a gap in rare cases for example in the case when some records
         * are gone due to retention.
         */
        void markNoMoreTracks() {
            this.noMoreTracks = true;
        }

        /**
         * @return true if all tracked offsets are acked and no more tracks are expected.
         */
        boolean isCompleted() {
            return bits.isEmpty() && (maxTrackedOffset == effectiveSize - 1 || noMoreTracks);
        }
    }
}
