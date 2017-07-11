package com.cloudera.fce.kafka.admin

import java.util.Properties

import joptsimple.OptionParser
import kafka.admin.AdminClient
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

object ConsumerOffsetCommand extends Logging {
  def main(args: Array[String]) {
    val opts = new ConsumerOffsetCommandOptions(args)
    opts.checkArgs()

    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapOpt)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, opts.groupOpt)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val client = createAdminClient(properties)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)

    if (opts.options.has(opts.listOpt)) {
      printOffsetsForGroup(getOffsetsForGroup(client, consumer, opts.options.valueOf(opts.groupOpt)))
    } else if (opts.options.has(opts.setOpt)) {
      setOffsetsForGroup(client, consumer, opts.options.valueOf(opts.groupOpt), opts.options.valueOf(opts.rewindOpt))
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

  class ConsumerOffsetCommandOptions(args: Array[String]) {
    val parser = new OptionParser

    val bootstrapOpt = parser.accepts("bootstrap-server")
      .withRequiredArg
      .describedAs("A list of host/port pairs to use for establishing the initial connection to the Kafka cluster")
      .ofType(classOf[String])

    val groupOpt = parser.accepts("group")
      .withRequiredArg
      .describedAs("The name of the consumer group")
      .ofType(classOf[String])

    val listOpt = parser.accepts("list")
      .withOptionalArg
      .describedAs("List the current offset for each partition / consumer in the group")
      .ofType(classOf[String])

    val setOpt = parser.accepts("set")
      .withOptionalArg
      .describedAs("Set the current offset for each partition / consumer in the group")
      .ofType(classOf[String])

    val rewindOpt = parser.accepts("rewind")
      .withOptionalArg
      .describedAs("The number to subtract from each partition / consumer in the group")
      .ofType(classOf[Long])

    val options = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapOpt)
      CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)
      if (!options.has(listOpt) && !options.has(setOpt)) {
        CommandLineUtils.printUsageAndDie(parser, s"Need to specify $listOpt or $setOpt")
      }
      if (options.has(setOpt) && !options.has(rewindOpt)) {
        CommandLineUtils.printUsageAndDie(parser, s"Option $setOpt is not valid without $rewindOpt")
      }
    }
  }

}
