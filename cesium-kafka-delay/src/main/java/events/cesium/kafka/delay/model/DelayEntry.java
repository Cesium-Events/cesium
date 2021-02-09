package events.cesium.kafka.delay.model;

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

    public Instant getDispatchInstant() {
        return Instant.ofEpochMilli(dispatchTimestamp);
    }

    public long getDispatchTimestamp() {
        return dispatchTimestamp;
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public long getDelay(TimeUnit unit) {
        // Return the number of millis until the dispatch time converted to the
        // specified unit
        return unit.convert(Instant.now().until(getDispatchInstant(), ChronoUnit.MILLIS), unit);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (dispatchTimestamp ^ (dispatchTimestamp >>> 32));
        result = prime * result + (int) (offset ^ (offset >>> 32));
        result = prime * result + partition;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DelayEntry other = (DelayEntry) obj;
        if (dispatchTimestamp != other.dispatchTimestamp)
            return false;
        if (offset != other.offset)
            return false;
        if (partition != other.partition)
            return false;
        return true;
    }

}
