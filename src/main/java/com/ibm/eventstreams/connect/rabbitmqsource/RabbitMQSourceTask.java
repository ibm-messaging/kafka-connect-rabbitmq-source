package com.ibm.eventstreams.connect.rabbitmqsource;

import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.EnvelopeSchema;
import com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord.SourceRecordConcurrentLinkedQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQSourceTask.class);

    RabbitMQSourceConnectorConfig config;
    SourceRecordConcurrentLinkedQueue records;

    private Channel channel;
    private Connection connection;

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return RabbitMQSourceConnector.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override public void start(Map<String, String> props) {
        this.config = new RabbitMQSourceConnectorConfig(props);
        this.records = new SourceRecordConcurrentLinkedQueue();
        ConnectConsumer consumer = new ConnectConsumer(this.records, this.config);

        ConnectionFactory connectionFactory = this.config.connectionFactory();
        try {
            log.info("Opening connection to {}:{}/{}", this.config.host, this.config.port, this.config.virtualHost);
            this.connection = connectionFactory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new ConnectException(e);
        }

        try {
            log.info("Creating Channel");
            this.channel = this.connection.createChannel();
        } catch (IOException e) {
            throw new ConnectException(e);
        }

        for (String queue : this.config.queues) {
            try {
                log.info("Setting channel.basicQos({}, {});", this.config.prefetchCount, this.config.prefetchGlobal);
                this.channel.basicQos(this.config.prefetchCount, this.config.prefetchGlobal);
                log.info("Starting consumer");
                this.channel.basicConsume(queue, consumer);
            } catch (IOException ex) {
                throw new ConnectException(ex);
            }
        }
    }

    /**
     * Poll this SourceTask for new records. This method should block if no data is currently
     * available.
     *
     * @return a list of source records
     */
    @Override public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> batch = new ArrayList<>(4096);

        while (!this.records.drain(batch)) {
            Thread.sleep(1000);
        }

        return batch;
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     *
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    @Override public void stop() {
        try {
            this.connection.close();
        } catch (IOException e) {
            log.error("Exception thrown while closing connection.", e);
        }
    }

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is received, or if a record is filtered by a transformation.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     *
     * @param record {@link SourceRecord} that was successfully sent via the producer.
     * @throws InterruptedException
     */
    @Override public void commitRecord(SourceRecord record) throws InterruptedException {
        Long deliveryTag = (Long) record.sourceOffset().get(EnvelopeSchema.FIELD_DELIVERYTAG);
        try {
            this.channel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }
}
