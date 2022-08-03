package events.cesium.kafka.delay.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayEntry implements Delayed {

    private final int partition;
    private final long offset;
    private final long dispatchTimestamp;

    public DelayEntry(int partition, long offset, long dispatchTimestamp) {
        super();

        this.partition = partition;
        this.offset = offset;
        this.dispatchTimestamp = dispatchTimestamp;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getDispatchTimestamp() {
        return dispatchTimestamp;
    }

    @Override
    public int compareTo(@Nonnull Delayed o) {
        DelayEntry entry = (DelayEntry) o;
        return Long.compare(getDispatchTimestamp(), entry.getDispatchTimestamp());
    }

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
        // Return the number of millis until the dispatch time converted to the
        // specified unit
        return getDispatchTimestamp() - System.currentTimeMillis();
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
