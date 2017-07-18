/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.fce.kafka.admin

import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSet}
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

    val client: AdminClient = createAdminClient(properties)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)

    if (opts.options.has(opts.listOpt))
      printOffsetsForGroup(getOffsetsForGroup(client, consumer, opts.options.valueOf(opts.groupOpt)))
    else if (opts.options.has(opts.rewindOffsetOpt))
      rewindOffsetsForGroup(client, consumer, opts.options.valueOf(opts.groupOpt),
        opts.options.valueOf(opts.rewindOffsetOpt))
    else {
      var timestamp = LocalDateTime.now()
      if (opts.options.has(opts.rewindTimestampOpt))
        timestamp.minusMinutes(opts.options.valueOf(opts.rewindTimestampOpt))
      else if (opts.options.has(opts.setTimestampOpt))
        timestamp = LocalDateTime.parse(opts.options.valueOf(opts.setTimestampOpt))

      setTimestampForGroup(client, consumer, opts.options.valueOf(opts.groupOpt),
        timestamp.atZone(ZoneId.systemDefault).toEpochSecond)
    }
  }

  /**
    * Rewind consumer group partition offsets
    *
    * @param client   Kafka admin client
    * @param consumer Kafka consumer
    * @param group    Name of the consumer group
    * @param rewind   Number to subtract from the current offset of each partition for the consumer group
    */
  def rewindOffsetsForGroup(client: AdminClient, consumer: KafkaConsumer[String, String], group: String, rewind: Long) {
    for (partitionOffset <- getOffsetsForGroup(client, consumer, group)) {
      logger.info(s"Rewinding offset by $rewind: " +
        s"group:${partitionOffset.group}, " +
        s"topic:${partitionOffset.topicPartition.topic}, " +
        s"partition:${partitionOffset.topicPartition.partition}, " +
        s"current offset:${partitionOffset.offset}, " +
        s"new offset:${partitionOffset.offset - rewind}")
      consumer.seek(partitionOffset.topicPartition, partitionOffset.offset - rewind)
      consumer.commitSync()
    }
  }

  /**
    * Set consumer group partition offsets based on timestamp
    *
    * @param client    Kafka admin client
    * @param consumer  Kafka consumer
    * @param group     Name of the consumer group
    * @param timestamp Timestamp to use to set the consumer group partition offsets
    */
  def setTimestampForGroup(client: AdminClient, consumer: KafkaConsumer[String, String], group: String, timestamp: Long) {
    val cgs = client.describeConsumerGroup(group)
    cgs.consumers match {
      case Some(summaries) =>
        for (summary <- summaries) {
          summary.assignment.foreach(tp => {
            val timestampOffset = consumer.offsetsForTimes(java.util.Collections.singletonMap(tp, timestamp)).get(tp).offset
            logger.info(s"Setting offset from timestamp $timestamp: " +
              s"group:$group, " +
              s"topic:${tp.topic}, " +
              s"partition:${tp.partition}, " +
              s"current offset:${consumer.committed(tp).offset}, " +
              s"new offset:$timestampOffset")
            consumer.seek(tp, timestampOffset)
            consumer.commitSync()
          })
        }
      case _ =>
    }
  }

  def printOffsetsForGroup(offsets: List[PartitionOffsets]) {
    println("%-20s %-20s %-10s %-15s".format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET"))
    for (offset <- offsets) {
      println("%-20s %-20s %-10s %-15s".format(
        offset.group,
        offset.topicPartition.topic,
        offset.topicPartition.partition,
        offset.offset))
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
    val cgs = client.describeConsumerGroup(group)
    var offsets = new ListBuffer[PartitionOffsets]()
    cgs.consumers match {
      case Some(summaries) =>
        for (summary <- summaries) {
          summary.assignment.foreach(tp => offsets += PartitionOffsets(group, tp, consumer.committed(tp).offset))
        }
      case _ =>
    }
    offsets.toList
  }

  def createAdminClient(props: Properties): AdminClient = {
    AdminClient.create(props)
  }

  case class PartitionOffsets(group: String, topicPartition: TopicPartition, offset: Long)

  class ConsumerOffsetCommandOptions(args: Array[String]) {
    val parser = new OptionParser

    val bootstrapOpt: ArgumentAcceptingOptionSpec[String] = parser.accepts("bootstrap-server")
      .withRequiredArg
      .describedAs("A list of host/port pairs to use for establishing the initial connection to the Kafka cluster")
      .ofType(classOf[String])

    val groupOpt: ArgumentAcceptingOptionSpec[String] = parser.accepts("group")
      .withRequiredArg
      .describedAs("The name of the consumer group")
      .ofType(classOf[String])

    val listOpt: ArgumentAcceptingOptionSpec[String] = parser.accepts("list")
      .withOptionalArg
      .describedAs("List the current offset for each partition / consumer in the group")
      .ofType(classOf[String])

    val rewindOffsetOpt: ArgumentAcceptingOptionSpec[Long] = parser.accepts("rewind_offset")
      .withOptionalArg
      .describedAs("The number to subtract from each partition / consumer in the group")
      .ofType(classOf[Long])

    val rewindTimestampOpt: ArgumentAcceptingOptionSpec[Long] = parser.accepts("rewind_timestamp")
      .withOptionalArg
      .describedAs("The number of seconds to subtract from each partition / consumer in the group")
      .ofType(classOf[Long])

    val setTimestampOpt: ArgumentAcceptingOptionSpec[String] = parser.accepts("set_timestamp")
      .withOptionalArg
      .describedAs("An ISO-8601 formatted timestamp to use to set the offset for each partition / consumer in the group")
      .ofType(classOf[String])

    val options: OptionSet = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapOpt)
      CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)

      if (options.has(listOpt))
        CommandLineUtils.checkInvalidArgs(parser, options, listOpt,
          Set(rewindOffsetOpt, rewindTimestampOpt, setTimestampOpt))
      else if (options.has(rewindOffsetOpt))
        CommandLineUtils.checkInvalidArgs(parser, options, rewindOffsetOpt,
          Set(listOpt, rewindTimestampOpt, setTimestampOpt))
      else if (options.has(rewindTimestampOpt))
        CommandLineUtils.checkInvalidArgs(parser, options, rewindTimestampOpt,
          Set(listOpt, rewindOffsetOpt, setTimestampOpt))
      else if (options.has(setTimestampOpt))
        CommandLineUtils.checkInvalidArgs(parser, options, setTimestampOpt,
          Set(listOpt, rewindOffsetOpt, rewindTimestampOpt))
      else
        CommandLineUtils.printUsageAndDie(parser, s"Must specify $listOpt or one of the [rewind_] or [set_] options")
    }
  }

}
