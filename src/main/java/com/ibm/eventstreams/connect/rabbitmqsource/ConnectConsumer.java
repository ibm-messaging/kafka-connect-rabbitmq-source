package com.ibm.eventstreams.connect.rabbitmqsource;

import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord.RabbitMQSourceRecordFactory;
import com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord.SourceRecordConcurrentLinkedQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ConnectConsumer implements Consumer {
    private static final Logger log = LoggerFactory.getLogger(ConnectConsumer.class);
    final SourceRecordConcurrentLinkedQueue records;
    final RabbitMQSourceConnectorConfig config;
    final RabbitMQSourceRecordFactory rabbitMQSourceRecordFactory;

    ConnectConsumer(SourceRecordConcurrentLinkedQueue records, RabbitMQSourceConnectorConfig config) {
        this.records = records;
        this.config = config;
        this.rabbitMQSourceRecordFactory = new RabbitMQSourceRecordFactory(this.config);
    }

    /**
     * Stores the most recently passed-in consumerTag - semantically, there should be only one.
     *
     * @param consumerTag - the consumer tag associated with the consumer
     */
    @Override public void handleConsumeOk(String s) {
        log.trace("handleConsumeOk({})", s);
    }

    /**
     * No-op implementation of Consumer.handleCancelOk(java.lang.String).
     *
     * @param consumerTag - the consumer tag associated with the consumer
     */
    @Override public void handleCancelOk(String s) {
        log.trace("handleCancelOk({})", s);
    }

    /**
     * No-op implementation of Consumer.handleCancel(String).
     *
     * @param consumerTag - the consumer tag associated with the consumer
     */
    @Override public void handleCancel(String s) throws IOException {
        log.trace("handleCancel({})", s);
    }

    /**
     * No-op implementation of Consumer.handleShutdownSignal(java.lang.String, com.rabbitmq.client.ShutdownSignalException).
     *
     * @param consumerTag - the consumer tag associated with the consumer
     */
    @Override public void handleShutdownSignal(String s, ShutdownSignalException e) {
        log.trace("handleShutdownSignal({}, {})", s, e);
    }

    /**
     * No-op implementation of Consumer.handleRecoverOk(java.lang.String).
     *
     * @param consumerTag - the consumer tag associated with the consumer
     */
    @Override public void handleRecoverOk(String s) {
        log.trace("handleRecoverOk({}, {})", s);
    }

    /**
     * No-op implementation of Consumer.handleDelivery(java.lang.String, com.rabbitmq.client.Envelope, com.rabbitmq.client.AMQP.BasicProperties, byte[]).
     *
     * @param consumerTag - the consumer tag associated with the consumer
     * @param envelope - packaging data for the message
     * @param properties - content header data for the message
     * @param body - the message body (opaque, client-specific byte array)
     */
    // Hereo1: Body Compose
    @Override public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
        log.trace("handleDelivery({})", consumerTag);

        SourceRecord sourceRecord = this.rabbitMQSourceRecordFactory.makeSourceRecord(consumerTag, envelope, basicProperties, bytes);
        this.records.add(sourceRecord);
    }
}