# Kafka Connect source connector for RabbitMQ
kafka-connect-rabbitmq-source is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) source connector for copying data from RabbitMQ into Apache Kafka.

The connector is supplied as source code which you can easily build into a JAR file.

## Installation

1. Clone the repository with the following command:

```bash
git@github.com:ibm-messaging/kafka-connect-rabbitmq-source.git
```

2. Change directory to the `kafka-connect-mq-source` directory:

```shell
cd rabbitmq-to-kafka
```

3. Build the connector using Maven:

```bash
mvn clean package
```

4. Setup a local zookeeper service running on port 2181 (default) 

5. Setup a local kafka service running on port 9092 (default)

6. Setup a local rabbitmq service running on port 15672 (default)

7. Copy the compiled jar file into the `/usr/local/share/java/` directory:

```bash
cp target/kafka-connect-rabbitmq-source-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/share/java/
```

8. Copy the `connect-standalone.properties` and `rabbitmq-source.properties` files into the `/usr/local/etc/kafka/` directory.

```bash
cp config/* /usr/local/etc/kafka/
```

9. Go to the kafka installation directory `/usr/local/etc/kafka/`:

```bash
cd /usr/local/etc/kafka/
```

10. Set the CLASSPATH value to `/usr/local/share/java/` as follows:

```bash
export CLASSPATH=/usr/local/share/java/
```

## Running in Standalone Mode

Run the following command to start the source connector service in standalone mode:

```bash
connect-standalone connect-standalone.properties rabbitmq-source.properties
```

## Running in Distributed Mode

1. In order to run the connector in distributed mode you must first register the connector with
Kafka Connect service by creating a JSON file in the format below:

```json
{
    "name": "RabbitMQSourceConnector",
    "config": {
        "connector.class": "com.ibm.eventstreams.connect.rabbitmqsource.RabbitMQSourceConnector",
        "tasks.max": "10",
        "kafka.topic" : "kafka_test",
        "rabbitmq.queue" : "rabbitmq_test",
        "rabbitmq.prefetch.count": "500",
        "rabbitmq.automatic.recovery.enabled": "true",
        "rabbitmq.network.recovery.interval.ms": "10000",
        "rabbitmq.topology.recovery.enabled": "true"
    }
}
```

A version of this file, `config/rabbitmq-source.json`, is located in the `config` directory.  To register
the connector do the following:

1. Run the following command to the start the source connector service in distributed mode:

```bash
connect-distributed connect-distributed.properties
```

2. Run the following command to register the connector with the Kafka Connect service:

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @config/rabbitmq-source.json http://localhost:8083/connector
```

You can verify that your connector was properly registered by going to `http://localhost:8083/connector` which 
should return a full list of available connectors.  This JSON connector profile will be propegated to all workers
across the distributed system.  After following these steps your connector will now run in distributed mode.

## Configuration

Create a target kafka topic named `kafka_test`:

```shell
kafka-topics --create --topic kafka_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

Go to the RabbitMQ site at the following URL: `http://localhost:15672/`

Create a new queue `rabbitmq_test`.

## Testing

Publish a new item to your RabbitMQ queue `rabbitmq_test` from the http://localhost:15672 webui console. 


Type in the following to view the contents of the `kafka_test` topic on kafka.

```shell
kafka-console-consumer --topic kafka_test --from-beginning --bootstrap-server 127.0.0.1:9092
```

## Issues and contributions
For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-jdbc-sink/issues). If you do want to submit a Pull Request related to this connector, please read the [contributing guide](CONTRIBUTING.md) first to understand how to sign your commits.


## License
Copyright 2020 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.
