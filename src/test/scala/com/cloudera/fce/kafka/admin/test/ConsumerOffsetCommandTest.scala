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

package com.cloudera.fce.kafka.admin.test

import java.util
import java.util.Properties

import com.cloudera.fce.kafka.admin.ConsumerOffsetCommand._
import kafka.admin.AdminClient
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}

class ConsumerOffsetCommandTest extends KafkaServerTestHarness {
  val bootstrap = "localhost:9092"
  val group = "test"
  val topic = "test"
  val properties = new Properties()
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, group)
  properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000")
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  var admin: AdminClient = _
  var consumer: KafkaConsumer[String, String] = _
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  override def generateConfigs(): Seq[KafkaConfig] = {
    val numServers = 1
    val overridingProps = new Properties()
    overridingProps.put("listeners", "PLAINTEXT://localhost:9092")
    TestUtils.createBrokerConfigs(numServers, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @Before
  override def setUp() {
    admin = createAdminClient(properties)
    consumer = new KafkaConsumer(properties)
    producer = TestUtils.createNewProducer[Array[Byte], Array[Byte]](bootstrap, maxBlockMs = 5000L)
    super.setUp()

    // TODO Figure out why this is needed
    Thread.sleep(500)
  }

  @After
  override def tearDown() {
    admin.close()
    consumer.close()
    producer.close()
    super.tearDown()
  }

  @Test
  def testGetOffsets() {
    createTestTopic()

    // Consume messages and set offsets
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(0)
    consumer.commitSync()

    val offsets: List[PartitionOffsets] = getOffsetsForGroup(admin, consumer, group)
    assertEquals(2, offsets.length) // Expect one item per topic partition
    offsets.foreach(partitionOffset => {
      assertEquals(group, partitionOffset.group)
      assertEquals(topic, partitionOffset.topicPartition.topic)
      assertEquals(25, partitionOffset.offset) // Expect both partitions to be consumed to latest
    })

    printOffsetsForGroup(offsets)

  }

  @Test
  def testSetOffsets() {
    createTestTopic()

    // Consume messages and set offsets
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(0)
    consumer.commitSync()

    // Rewind offsets by 10
    rewindOffsetsForGroup(admin, consumer, group, 10L)

    val offsets: List[PartitionOffsets] = getOffsetsForGroup(admin, consumer, group)
    assertEquals(2, offsets.length) // Expect one item per topic partition
    offsets.foreach(partitionOffset => {
      assertEquals(group, partitionOffset.group)
      assertEquals(topic, partitionOffset.topicPartition.topic)
      assertEquals(15, partitionOffset.offset) // Expect both partitions to have been reset to 15
    })
    printOffsetsForGroup(offsets)
  }

  @Test
  def testSetTimestamp() {
    createTestTopic()

    // Consume messages and set offsets
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(0)
    consumer.commitSync()

    // Rewind offsets to timestamp
    setTimestampForGroup(admin, consumer, group, 10L)

    val offsets: List[PartitionOffsets] = getOffsetsForGroup(admin, consumer, group)
    assertEquals(2, offsets.length) // Expect one item per topic partition
    assertEquals(4, offsets(0).offset)
    assertEquals(5, offsets(1).offset)
    printOffsetsForGroup(offsets)
  }

  private def createTestTopic() {
    object callback extends Callback {
      def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception != null) {
          fail("Send callback returned the following exception", exception)
        }
      }
    }
    try {
      logger.debug(s"Creating topic:$topic")
      TestUtils.createTopic(zkUtils, topic, 2, 1, servers)
      for (x <- 1 to 50) {
        val msg = new ProducerRecord(topic, x % 2, x.toLong, s"key$x".getBytes, s"value$x".getBytes)
        producer.send(msg, callback)
        logger.debug(s"Sending message: key:key$x, value:value$x, partition:${x % 2}, timestamp:${x.toLong}")
      }
    } finally {
      producer.close()
    }
  }
}
