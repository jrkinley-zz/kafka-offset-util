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

import java.security.Permission

import com.cloudera.fce.kafka.admin.ConsumerOffsetCommand.ConsumerOffsetCommandOptions
import junit.framework.TestCase
import org.junit.Assert._
import org.junit.{After, Before, Test}

class CommandOptionsTest extends TestCase {
  val bootstrapServer = "--bootstrap-server=localhost:9092"
  val group = "--group=test"
  val rewindOffset = "--rewind_offset=1000"
  val rewindTimestamp = "--rewind_timestamp=3600"
  val list = "--list"

  val noArgs = Array("")
  val listArgs = Array(bootstrapServer, group, list)
  val listArgsBad1 = Array(group, list)
  val listArgsBad2 = Array(bootstrapServer, list)
  val setArgs = Array(bootstrapServer, group, rewindOffset)
  val setArgsBad1 = Array(bootstrapServer, group)
  val setArgsBad2 = Array(bootstrapServer, group, rewindOffset, rewindTimestamp)

  @Before
  override def setUp() {
    super.setUp()
    System.setSecurityManager(new NoSystemExit)
  }

  @After
  override def tearDown() {
    super.tearDown()
    System.setSecurityManager(null)
  }

  @Test
  def testCheckArgs() {
    val opts1 = new ConsumerOffsetCommandOptions(noArgs)
    try {
      opts1.checkArgs()
      fail("Did not exit with zero args")
    } catch {
      case _: ExitException =>
    }

    val opts2 = new ConsumerOffsetCommandOptions(listArgs)
    try {
      opts2.checkArgs()
    } catch {
      case _: ExitException =>
        fail(s"list args are valid")
    }

    val opts3 = new ConsumerOffsetCommandOptions(listArgsBad1)
    try {
      opts3.checkArgs()
      fail("'bootstrap-server' arg is mandatory")
    } catch {
      case _: ExitException =>
    }

    val opts4 = new ConsumerOffsetCommandOptions(listArgsBad2)
    try {
      opts4.checkArgs()
      fail("'group' arg is mandatory")
    } catch {
      case _: ExitException =>
    }

    val opts5 = new ConsumerOffsetCommandOptions(setArgs)
    try {
      opts5.checkArgs()
    } catch {
      case ee: ExitException =>
        fail(s"args: '$setArgs' are valid.")
    }

    val opts6 = new ConsumerOffsetCommandOptions(setArgsBad1)
    try {
      opts6.checkArgs()
      fail("Must specify 'rewind_offset', 'rewind_timestamp', or 'set_timestamp' with set")
    } catch {
      case _: ExitException =>
    }

    val opts7 = new ConsumerOffsetCommandOptions(setArgsBad2)
    try {
      opts7.checkArgs()
      fail("Specifying both 'rewind_offset' and 'rewind_timestamp' is invalid with set")
    } catch {
      case _: ExitException =>
    }
  }

  case class ExitException(status: Int) extends SecurityException

  class NoSystemExit extends SecurityManager {
    override def checkExit(status: Int) {
      super.checkExit(status)
      throw ExitException(status)
    }

    override def checkPermission(perm: Permission) {}
  }

}
