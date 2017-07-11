package com.cloudera.fce.kafka.admin.test

import java.security.Permission

import com.cloudera.fce.kafka.admin.ConsumerOffsetCommand.ConsumerOffsetCommandOptions
import junit.framework.TestCase
import org.junit.Assert._
import org.junit.{After, Before, Test}

class CommandOptionsTest extends TestCase {
  val bootstrapServer = "--bootstrap-server=localhost:9092"
  val group = "--group=test"
  val rewind = "--rewind=1000"
  val list = "--list"
  val set = "--set"

  val noArgs = Array("")
  val listArgs = Array(bootstrapServer, group, list)
  val listArgsBad1 = Array(group, list)
  val listArgsBad2 = Array(bootstrapServer, list)
  val setArgs = Array(bootstrapServer, group, set, rewind)
  val setArgsBad = Array(bootstrapServer, group, set)

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

    val opts6 = new ConsumerOffsetCommandOptions(setArgsBad)
    try {
      opts6.checkArgs()
      fail("'rewind' arg is mandatory with set")
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
