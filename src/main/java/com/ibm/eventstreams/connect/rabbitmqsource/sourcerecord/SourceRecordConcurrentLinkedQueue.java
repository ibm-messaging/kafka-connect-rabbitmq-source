package com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Queue used to buffer records from systems using listener threads.
 */
public class SourceRecordConcurrentLinkedQueue extends ConcurrentLinkedQueue<SourceRecord> {
    private static final Logger log = LoggerFactory.getLogger(SourceRecordConcurrentLinkedQueue.class);

    private final int batchSize;
    private final int timeout;

    /**
     * Constructor creates a new INSTANCE of the SourceRecordConcurrentLinkedQueue
     *
     * @param batchSize The maximum number of records to return per batch.
     * @param timeout   The amount of time to wait if no batch returned.
     */
    public SourceRecordConcurrentLinkedQueue(int batchSize, int timeout) {
        this.batchSize = batchSize;
        this.timeout = timeout;
    }

    /**
     * Constructor creates a new INSTANCE of the SourceRecordConcurrentLinkedQueue with a batchSize of 1024 and timeout of 0.
     */
    public SourceRecordConcurrentLinkedQueue() {
        this(1024, 0);
    }

    /**
     * Method used to drain the records from the deque in order and add them to the supplied list.
     *
     * @param records list to add the records to.
     * @return true if records added to the list, false if not.
     * @throws InterruptedException Thrown if thread killed while sleeping.
     */
    public boolean drain(List<SourceRecord> records) throws InterruptedException {
        return drain(records, this.timeout);
    }

    /**
     * Method used to drain the records from the queue in order and add them to the supplied list.
     *
     * @param records list to add the records to.
     * @param timeout amount of time to sleep if no records added.
     * @return true if records added to the list, false if not.
     * @throws InterruptedException     Thrown if thread killed while sleeping.
     * @throws IllegalArgumentException Thrown if timeout is less than 0.
     */
    public boolean drain(List<SourceRecord> records, int timeout) throws InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("determining size for this run. batchSize={}, records.size()={}", this.batchSize, records.size());
        }

        int numOfRecordsToProcess = Math.min(this.batchSize, this.size());

        if (log.isDebugEnabled()) {
            log.debug("Draining {} record(s).", numOfRecordsToProcess);
        }

        while (numOfRecordsToProcess > 0) {
            SourceRecord record = this.poll();

            if (record != null) {
                records.add(record);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Poll returned null. exiting");
                    break;
                }
            }
            numOfRecordsToProcess-=1;
        }

        if (records.isEmpty() && timeout > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Found no records, sleeping {} ms.", timeout);
            }
            Thread.sleep(timeout);
        }

        return !records.isEmpty();
    }
}
