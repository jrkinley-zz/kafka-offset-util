package com.cloudera.fce.kafka.admin

import java.util.Properties

import kafka.admin.AdminClient
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

object ConsumerOffsetCommand extends Logging {
  def main(args: Array[String]) {
    val bootstrap = args(0)
    val group = args(1)
    val cmd = args(2)
    val rewind = args(3).toLong

    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val client = createAdminClient(properties)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)

    cmd match {
      case "describe" => printOffsetsForGroup(getOffsetsForGroup(client, consumer, group))
      case "update" => setOffsetsForGroup(client, consumer, group, rewind)
      case _ => throw new Exception()
    }
  }

  /**
    * Rewind partition offsets for a consumer group
    *
    * @param client   Kafka admin client
    * @param consumer Kafka consumer
    * @param group    Name of the consumer group
    * @param rewind   Number to subtract from the current offset of each partition for the consumer group
    */
  def setOffsetsForGroup(client: AdminClient, consumer: KafkaConsumer[String, String], group: String, rewind: Long) {
    for (partitionOffset <- getOffsetsForGroup(client, consumer, group)) {
      logger.info(s"setting offset: group:${partitionOffset.group}, " +
        s"topic:${partitionOffset.topicPartition.topic}, " +
        s"partition:${partitionOffset.topicPartition.partition}, " +
        s"current offset:${partitionOffset.offset}, " +
        s"new offset:${partitionOffset.offset - rewind}")
      consumer.seek(partitionOffset.topicPartition, partitionOffset.offset - rewind)
      consumer.commitSync()
    }
  }

  def printOffsetsForGroup(offsets: List[PartitionOffsets]) {
    println("%-20s %-20s %-10s %-15s".format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET"))
    for (offset <- offsets) {
      println("%-20s %-20s %-10s %-15s".format(offset.group,
        offset.topicPartition.topic, offset.topicPartition.partition, offset.offset))
    }
  }

  /**
    * Retrieve a list of topic partitions and their current offsets for a consumer group
    *
    * @param client   Kafka admin client
    * @param consumer Kafka consumer
    * @param group    Name of the consumer group
    * @return List of topic partitions and their current offsets for the consumer group
    */
  def getOffsetsForGroup(client: AdminClient, consumer: KafkaConsumer[String, String], group: String): List[PartitionOffsets] = {
    client.describeConsumerGroup(group) match {
      case None => throw new Exception(s"consumer group:$group does not exist.")
      case Some(consumerSummaries) =>
        if (consumerSummaries.isEmpty)
          throw new Exception(s"consumer group:$group is rebalancing.")
        else {
          var offsets = new ListBuffer[PartitionOffsets]()
          for (summary <- consumerSummaries)
            for (tp <- summary.assignment) {
              val offset = consumer.committed(tp).offset
              logger.debug(s"group:$group, topic:${tp.topic}, partition:${tp.partition}, offset:$offset")
              offsets += PartitionOffsets(group, tp, offset)
            }
          offsets.toList
        }
    }
  }

  def createAdminClient(props: Properties): AdminClient = {
    AdminClient.create(props)
  }

  case class PartitionOffsets(group: String, topicPartition: TopicPartition, offset: Long)

}
