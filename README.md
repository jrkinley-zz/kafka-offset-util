# kafka-offset-util
Utility for managing consumer offsets in Kafka

```
Usage: com.cloudera.fce.kafka.admin.ConsumerOffsetCommand
  <bootstrap>  A list of host:port pairs to connect to the Kafka cluster
               e.g. `broker1:9092,broker2:9092,brokerX:9092` 
  <group       The name of the consumer group
  <cmd>        The command to execute: `describe` or `update`
  <rewind>     The number to subtract from the current offset of each partition for the consumer group
```
