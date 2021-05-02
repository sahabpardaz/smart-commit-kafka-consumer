package ir.sahab.kafkaconsumer;

import java.util.OptionalLong;
import org.junit.Assert;
import org.junit.Test;

public class OffsetTrackerTest {

    @Test
    public void testTrackWithMargin() {
        final int pageSize = 5;
        final int maxOpenPagesPerPartition = 2;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page starting from an initial margin: [233..234]
        offsetTracker.track(partition, 233);
        offsetTracker.track(partition, 234);

        // Track calls which opens the second page: [235..239]
        offsetTracker.track(partition, 235);
        offsetTracker.track(partition, 236);

        // Offset 233 does not complete any page.
        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 233);
        Assert.assertFalse(offsetToCommit.isPresent());

        // Offset 234 makes the first page complete.
        offsetToCommit = offsetTracker.ack(partition, 234);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(235, offsetToCommit.getAsLong());
    }

    @Test
    public void testDisorderedAckOffsets() {
        final int pageSize = 4;
        final int maxOpenPagesPerPartition = 2;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page: [0..3]
        offsetTracker.track(partition, 0);
        offsetTracker.track(partition, 1);
        offsetTracker.track(partition, 2);
        offsetTracker.track(partition, 3);

        // Disordered acks: the last ack of this pages, should make it complete.
        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 0);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 3);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 2);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 1);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(4, offsetToCommit.getAsLong());
    }

    @Test
    public void testDisorderedAckPages() {
        final int pageSize = 2;
        final int maxOpenPagesPerPartition = 10;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page: [0..1]
        offsetTracker.track(partition, 0);
        offsetTracker.track(partition, 1);

        // Track calls which opens the first page: [2..3]
        offsetTracker.track(partition, 2);
        offsetTracker.track(partition, 3);

        // Track calls which opens the first page: [4..5]
        offsetTracker.track(partition, 4);
        offsetTracker.track(partition, 5);

        // Disordered acks: second page gets completed first, so we should get the commit offset,
        // after the first page is completed.
        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 4);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 1);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 3);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 2);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 0);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(4, offsetToCommit.getAsLong());
    }

    @Test
    public void testPageWithTailGap() {
        final int pageSize = 3;
        final int maxOpenPagesPerPartition = 2;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page: [0..2]
        offsetTracker.track(partition, 1);

        // Track calls which opens the second page: [3..5]
        // Offset 1 completes the first page because the next offsets are inside the recognized gap.
        offsetTracker.track(partition, 3);


        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 1);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(3, offsetToCommit.getAsLong());

        offsetToCommit = offsetTracker.ack(partition, 3);
        Assert.assertFalse(offsetToCommit.isPresent());
    }

    @Test
    public void testGapMiddleOfPage() {
        final int pageSize = 3;
        final int maxOpenPagesPerPartition = 2;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // The first call opens the first page: [0..2]. The missing track for offset=1, indicates a gap inside the page.
        offsetTracker.track(partition, 0);
        offsetTracker.track(partition, 2);

        OptionalLong offsetToCommit;
        // Offset 0 does not complete the page
        offsetToCommit = offsetTracker.ack(partition, 0);
        Assert.assertFalse(offsetToCommit.isPresent());

        // Offset 2 completes the page because the offset 1 is inside the recognized gap.
        offsetToCommit = offsetTracker.ack(partition, 2);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(3, offsetToCommit.getAsLong());
    }

    @Test
    public void testPageWithGapWhenNoAckIsRemained() {
        final int pageSize = 3;
        final int maxOpenPagesPerPartition = 2;
        final OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page: [0..2]
        offsetTracker.track(partition, 0);
        offsetTracker.track(partition, 1);

        // Ack all of the tracked offset so far
        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 0);
        Assert.assertFalse(offsetToCommit.isPresent());
        offsetToCommit = offsetTracker.ack(partition, 1);
        Assert.assertFalse(offsetToCommit.isPresent());

        // Track calls which opens the second page: [3..5] and makes a gap for the first page.  Because we have no other
        // ack for the first page, we can just get  the commit offset on completion of the next page.
        offsetTracker.track(partition, 3);
        offsetTracker.track(partition, 4);
        offsetTracker.track(partition, 5);

        // Offset 3 , 4 does not make the second page complete.
        offsetToCommit = offsetTracker.ack(partition, 3);
        Assert.assertFalse(offsetToCommit.isPresent());
        offsetToCommit = offsetTracker.ack(partition, 4);
        Assert.assertFalse(offsetToCommit.isPresent());

        // Offset 5 make the second page complete and because we have no remaining offset in the first page, we will get
        // a commit offset here.
        offsetToCommit = offsetTracker.ack(partition, 5);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(6, offsetToCommit.getAsLong());
    }

    @Test
    public void testPartitionFull() {
        int pageSize = 2;
        int maxOpenPagesPerPartition = 2;
        OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // Track calls which opens the first page: [0..1]
        Assert.assertTrue(offsetTracker.track(partition, 0));
        Assert.assertTrue(offsetTracker.track(partition, 1));

        // Track calls which opens the second page: [2..3]
        Assert.assertTrue(offsetTracker.track(partition, 2));
        Assert.assertTrue(offsetTracker.track(partition, 3));

        // Next track call is failed because the maximum open pages (i.e., 2) is reached.
        Assert.assertFalse(offsetTracker.track(partition, 4));

        // But calling track for other partitions, is successful.
        Assert.assertTrue(offsetTracker.track(1, 0));

        // These acks make the first page completed.
        offsetTracker.ack(partition, 0);
        offsetTracker.ack(partition, 1);

        // So, we can have successful track again.
        Assert.assertTrue(offsetTracker.track(partition, 4));
    }

    @Test
    public void testReset() {
        int pageSize = 2;
        int maxOpenPagesPerPartition = 5;
        OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // These tracks make two pages open.
        offsetTracker.track(partition, 0);
        offsetTracker.track(partition, 1);
        offsetTracker.track(partition, 2);
        offsetTracker.track(partition, 3);

        // These acks make the pages partially completed.
        offsetTracker.ack(partition, 0);
        offsetTracker.ack(partition, 2);

        // Reset makes our history clear.
        offsetTracker.reset();

        // After reset, we do not get the previous offsets. The world is changed!
        // These calls opens a new page with another range of offsets: [210..211]
        offsetTracker.track(partition, 210);
        offsetTracker.track(partition, 211);

        // It may be one ack which is received with delay (after reset).
        // Offset tracker is not sensitive to these kind of acks.
        offsetTracker.ack(partition, 1);

        // We do not need to get acks about the pages which is opened before reset.
        // Getting acks of the new opened pages (after reset), should result in an offset to commit.
        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 210);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetToCommit = offsetTracker.ack(partition, 211);
        Assert.assertTrue(offsetToCommit.isPresent());
        Assert.assertEquals(212, offsetToCommit.getAsLong());
    }

    @Test
    public void testResetWhenSomeBufferedRecordsFromPreviousSession() {
        int pageSize = 3;
        int maxOpenPagesPerPartition = 5;
        OffsetTracker offsetTracker = new OffsetTracker(pageSize, maxOpenPagesPerPartition);
        final int partition = 0;

        // These tracks make two pages open.
        offsetTracker.track(partition, 0);  // From first session
        offsetTracker.track(partition, 1);  // From first session
        offsetTracker.track(partition, 2);  // From first session
        offsetTracker.track(partition, 3);  // From first session

        // These acks make the pages partially completed.
        offsetTracker.ack(partition, 0);  // For first session
        offsetTracker.ack(partition, 1);  // For first session
        offsetTracker.ack(partition, 2);  // For first session

        // We will reset offset tracker to simulate partitions re-balance.
        offsetTracker.reset();

        // We are assuming that we have a record buffered from previous session that we get its
        // track here. It opens a new partial page.
        offsetTracker.track(partition, 4);  // Buffered from first session

        // And these records are from the second session. Note that they are started from the
        // last committed offset of the first session.
        offsetTracker.track(partition, 3);  // Now we have this offset twice in pending records.
        offsetTracker.track(partition, 4);  // Now we have this offset twice in pending records.
        offsetTracker.track(partition, 5);
        offsetTracker.track(partition, 6);
        offsetTracker.track(partition, 7);
        offsetTracker.track(partition, 8);
        offsetTracker.track(partition, 9);
        offsetTracker.track(partition, 10);
        offsetTracker.track(partition, 11);

        OptionalLong offsetToCommit;
        offsetToCommit = offsetTracker.ack(partition, 3);  // Ack #1 for offset 3
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetTracker.ack(partition, 6);
        offsetTracker.ack(partition, 7);
        offsetToCommit = offsetTracker.ack(partition, 8);
        Assert.assertFalse(offsetToCommit.isPresent());

        offsetTracker.ack(partition, 4);   // Ack #1 for offset 4
        offsetTracker.ack(partition, 3);   // Ack #2 for offset 3
        offsetTracker.ack(partition, 4);   // Ack #2 for offset 4
        offsetToCommit = offsetTracker.ack(partition, 5);
        Assert.assertEquals(9, offsetToCommit.getAsLong());

        offsetTracker.ack(partition, 9);
        offsetTracker.ack(partition, 10);
        offsetToCommit = offsetTracker.ack(partition, 11);
        Assert.assertEquals(12, offsetToCommit.getAsLong());

    }
}