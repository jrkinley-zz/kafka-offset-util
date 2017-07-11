# kafka-offset-util
Utility for managing consumer offsets in Kafka

```
Usage: com.cloudera.fce.kafka.admin.ConsumerOffsetCommand

--bootstrap-server  A list of host/port pairs to use for establishing 
                    the initial connection to the Kafka cluster>                            
--group             The name of the consumer group                                           
--list              List the current offset for each partition / 
                    consumer in the group
--rewind            Long: The number to subtract from each partition / 
                    consumer in the group (use with --set)
--set               Set the current offset for each partition / 
                    consumer in the group (use with --rewind)
```
