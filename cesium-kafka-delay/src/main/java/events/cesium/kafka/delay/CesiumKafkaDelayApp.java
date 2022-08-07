package events.cesium.kafka.delay;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import events.cesium.kafka.delay.worker.DelayTrackerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import events.cesium.kafka.delay.model.DelayEntry;
import events.cesium.kafka.delay.worker.DelayTrackerWorker;

public class CesiumKafkaDelayApp {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final CesiumConfig cesiumConfig;

    private DelayTrackerWorker delayTrackerWorker;

    private final DelayQueue<DelayEntry> delayQueue = new DelayQueue<>();
    private final Lock sharedLock = new ReentrantLock();

    public CesiumKafkaDelayApp(CesiumConfig cesiumConfig) {
        super();
        this.cesiumConfig = cesiumConfig;
    }

    public synchronized void startProcessingDelays() {
        // Create the tracker consumer worker to fill up the delay queue in
        // initialization
        delayTrackerWorker = new DelayTrackerWorker(cesiumConfig);
        delayTrackerWorker.beginProcessing();
    }

    public synchronized void stopProcessingDelays() {
        delayTrackerWorker.triggerShutdown();
    }

    public static void main(String[] args) {
        CesiumKafkaDelayApp app = new CesiumKafkaDelayApp(new CesiumConfig(System.getProperty("CesiumEnv")));

        app.startProcessingDelays();

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            app.stopProcessingDelays();
        }
    }
}
