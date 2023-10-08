/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc

import java.net.ServerSocket
import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model.{ExposedPort, Frame, HostConfig, PortBinding}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.DockerUtils

abstract class DatabaseOnDocker {
  /**
   * The docker image to be pulled.
   */
  val imageName: String

  /**
   * Environment variables to set inside of the Docker container while launching it.
   */
  val env: Map[String, String]

  /**
   * Whether or not to use ipc mode for shared memory when starting docker image
   */
  val usesIpc: Boolean

  /**
   * The container-internal JDBC port that the database listens on.
   */
  val jdbcPort: Int

  /**
   * Parameter whether the container should run privileged.
   */
  val privileged: Boolean = false

  /**
   * Return a JDBC URL that connects to the database running at the given IP address and port.
   */
  def getJdbcUrl(ip: String, port: Int): String

  /**
   * Return the JDBC properties needed for the connection.
   */
  def getJdbcProperties(): Properties = new Properties()

  /**
   * Optional entry point when container starts
   *
   * Startup process is a parameter of entry point. This may or may not be considered during
   * startup. Prefer entry point to startup process when you need a command always to be executed or
   * you want to change the initialization order.
   */
  def getEntryPoint: Option[String] = None

  /**
   * Optional process to run when container starts
   */
  def getStartupProcessName: Option[String] = None

  /**
   * Optional step before container starts
   */
  def beforeContainerStart(
      hostConfig: HostConfig,
      createContainerCmd: CreateContainerCmd): Unit = {}
}

abstract class DockerJDBCIntegrationSuite
  extends SharedSparkSession with Eventually with DockerIntegrationFunSuite {

  protected val dockerIp = DockerUtils.getDockerIp()
  val db: DatabaseOnDocker
  val connectionTimeout = timeout(5.minutes)
  val keepContainer =
    sys.props.getOrElse("spark.test.docker.keepContainer", "false").toBoolean
  val removePulledImage =
    sys.props.getOrElse("spark.test.docker.removePulledImage", "true").toBoolean

  private var docker: DockerClient = _
  // Configure networking (necessary for boot2docker / Docker Machine)
  protected lazy val externalPort: Int = {
    val sock = new ServerSocket(0)
    val port = sock.getLocalPort
    sock.close()
    port
  }
  private var containerId: String = _
  private var pulled: Boolean = false
  protected var jdbcUrl: String = _

  override def beforeAll(): Unit = runIfTestsEnabled(s"Prepare for ${this.getClass.getName}") {
    super.beforeAll()
    try {
      val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
      val httpClient = new ZerodepDockerHttpClient.Builder().dockerHost(config.getDockerHost)
        .sslConfig(config.getSSLConfig).build
      docker = DockerClientImpl.getInstance(config, httpClient)
      // Check that Docker is actually up
      try {
        docker.pingCmd().exec()
      } catch {
        case NonFatal(e) =>
          log.error("Exception while connecting to Docker. Check whether Docker is running.")
          throw e
      }
      // Ensure that the Docker image is installed:
      try {
        docker.inspectImageCmd(db.imageName).exec()
      } catch {
        case _: NotFoundException =>
          log.warn(s"Docker image ${db.imageName} not found; pulling image from registry")
          docker.pullImageCmd(db.imageName).start().awaitCompletion()
          pulled = true
      }

      val hostConfig = HostConfig.newHostConfig()
        .withPrivileged(db.privileged)
        .withNetworkMode("bridge")
        .withIpcMode(if (db.usesIpc) "host" else "")
        .withPortBindings(PortBinding.parse(s"$dockerIp:$externalPort:${db.jdbcPort}/tcp"))
      // Create the database container:
      val createContainerCmd = docker.createContainerCmd(db.imageName).withNetworkDisabled(false)
        .withEnv(db.env.map { case (k, v) => s"$k=$v" }.toSeq.asJava)
        .withExposedPorts(ExposedPort.tcp(db.jdbcPort))
      if (db.getEntryPoint.isDefined) {
        createContainerCmd.withEntrypoint(db.getEntryPoint.get)
      }
      if (db.getStartupProcessName.isDefined) {
        createContainerCmd.withCmd(db.getStartupProcessName.get)
      }
      db.beforeContainerStart(hostConfig, createContainerCmd)
      createContainerCmd.withHostConfig(hostConfig)
      // Create the database container:
      containerId = createContainerCmd.exec().getId
      // Start the container and wait until the database can accept JDBC connections:
      docker.startContainerCmd(containerId).exec()
      jdbcUrl = db.getJdbcUrl(dockerIp, externalPort)
      var conn: Connection = null
      eventually(connectionTimeout, interval(1.second)) {
        conn = getConnection()
      }
      // Run any setup queries:
      try {
        dataPreparation(conn)
      } finally {
        conn.close()
      }
    } catch {
      case NonFatal(e) =>
        try {
          afterAll()
        } finally {
          throw e
        }
    }
  }

  override def afterAll(): Unit = {
    try {
      cleanupContainer()
    } finally {
      if (docker != null) {
        docker.close()
      }
      super.afterAll()
    }
  }

  /**
   * Return the JDBC connection.
   */
  def getConnection(): Connection = {
    DriverManager.getConnection(jdbcUrl, db.getJdbcProperties())
  }

  /**
   * Prepare databases and tables for testing.
   */
  def dataPreparation(connection: Connection): Unit

  private def cleanupContainer(): Unit = {
    if (docker != null && containerId != null && !keepContainer) {
      try {
        docker.killContainerCmd(containerId).exec()
      } catch {
        case NonFatal(e) =>
          val exitContainerIds = docker.listContainersCmd().withStatusFilter(Seq("exited").asJava)
            .exec().asScala.map(_.getId)
          if (exitContainerIds.contains(containerId)) {
            logWarning(s"Container $containerId already stopped")
          } else {
            logWarning(s"Could not stop container $containerId", e)
          }
      } finally {
        logContainerOutput()
        docker.removeContainerCmd(containerId).exec()
        if (removePulledImage && pulled) {
          docker.removeImageCmd(db.imageName).exec()
        }
      }
    }
  }

  private def logContainerOutput(): Unit = {
    logInfo("\n\n===== CONTAINER LOGS FOR container Id: " + containerId + " =====")
    val logContainerCmd = docker.logContainerCmd(containerId).withStdOut(true).withStdErr(true)
      .exec(new ResultCallback.Adapter[Frame]() {
        override def onNext(frame: Frame): Unit = {
          logInfo(frame.toString)
        }
      }).awaitCompletion()
    try {
      logInfo("\n\n===== END OF CONTAINER LOGS FOR container Id: " + containerId + " =====")
    } finally {
      logContainerCmd.close()
    }
  }
}
