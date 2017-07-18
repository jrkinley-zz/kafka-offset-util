> **Deprecated:**
> The features provided by this tool have been added to Apache Kafka 0.11.0.0 by [KAFKA-4743](https://issues.apache.org/jira/browse/KAFKA-4743)

# kafka-offset-util
Utility for managing consumer offsets in Kafka

```
Usage: com.cloudera.fce.kafka.admin.ConsumerOffsetCommand

--bootstrap-server  A list of host/port pairs to use for establishing the initial connection to the Kafka cluster
--group             The name of the consumer group                                           
--list              List the current offset for each consumer / partition in the group
--rewind_offset     Long: The number to subtract from each consumer / partition in the group
--rewind_timestamp  Long: The number of seconds to subtract from each consumer / partition in the group
--set_timestamp     An ISO-8601 formatted timestamp to use to set the   offset for each consumer / partition in the group
```
