package events.cesium.delayqueue;

import events.cesium.kafka.delay.model.DelayEntry;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DelayQueueTest {

    @Test
    public void testDelayQueue() throws InterruptedException {
        DelayQueue<DelayEntry> delayQueue = new DelayQueue<>();

        long start = Instant.now().toEpochMilli();

        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 100000; j++) {
                DelayEntry entry = new DelayEntry(i, j, start + (i * 1000));
                delayQueue.put(entry);
            }
        }

        assertEquals(1000000, delayQueue.size());

        long maxDelay = 0;
        DelayEntry entry;
        while ((entry = delayQueue.poll(1, TimeUnit.SECONDS)) != null) {
            long now = System.currentTimeMillis();
            maxDelay = Math.max(maxDelay, now - entry.getDispatchTimestamp());
            assertTrue(now >= entry.getDispatchTimestamp());
        }
        System.out.println("Max dispatch delay: " + maxDelay);

        assertEquals(0, delayQueue.size());
    }
}
