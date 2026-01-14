package org.sunbird.obsrv.router

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.{InMemoryReporter, MiniClusterResourceConfiguration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.core.model.Models.SystemEvent
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.router.task.{DynamicRouterConfig, DynamicRouterStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.duration._

class DynamicRouterStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  private val metricsReporter = InMemoryReporter.createWithRetainedMetrics
  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(metricsReporter.addToConfiguration(new Configuration()))
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val routerConfig = new DynamicRouterConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(routerConfig)
  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()(embeddedKafkaConfig)
    val postgresConnect = new PostgresConnect(postgresConfig)
    insertTestData(postgresConnect)
    postgresConnect.closeConnection()
    createTestTopics()
    publishMessagesToKafka()
    flinkCluster.before()
  }

  private def publishMessagesToKafka(): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.SUCCESS_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.FAILED_EVENT)
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("update datasets set dataset_config = '" + """{"data_key":"id","timestamp_key":"date1","entry_topic":"ingest"}""" + "' where id='d2';")

  }

  override def afterAll(): Unit = {

    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      routerConfig.kafkaSystemTopic, routerConfig.kafkaInputTopic, "d1-events", routerConfig.kafkaFailedTopic
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "DynamicRouterStreamTaskTestSpec" should "validate the router stream task" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(routerConfig)
    val task = new DynamicRouterStreamTask(routerConfig, kafkaConnector)
    task.process(env)
    env.executeAsync(routerConfig.jobName)

    val outputs = EmbeddedKafka.consumeNumberMessagesFrom[String]("d1-events", 1, timeout = 30.seconds)
    validateOutputs(outputs)

    val failedEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](routerConfig.kafkaFailedTopic, 1, timeout = 30.seconds)
    validateFailedEvents(failedEvents)

    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](routerConfig.kafkaSystemTopic, 2, timeout = 30.seconds)
    validateSystemEvents(systemEvents)

    validateMetrics(metricsReporter)
  }

  private def validateOutputs(outputs: List[String]): Unit = {
    outputs.size should be(1)
    Console.println("Output", outputs.head)
  }

  private def validateFailedEvents(failedEvents: List[String]): Unit = {
    failedEvents.size should be(1)
    Console.println("Output", failedEvents.head)
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(2)

    systemEvents.foreach(se => {
      val event = JSONUtil.deserialize[SystemEvent](se)
      val error = event.data.error
      if (event.ctx.dataset.getOrElse("ALL").equals("ALL"))
        event.ctx.dataset_type should be(None)
      else if (error.isDefined) {
        val errorCode = error.get.error_code
        if (errorCode.equals(ErrorConstants.MISSING_DATASET_ID.errorCode) ||
          errorCode.equals(ErrorConstants.MISSING_DATASET_CONFIGURATION.errorCode) ||
          errorCode.equals(ErrorConstants.EVENT_MISSING.errorCode)) {
          event.ctx.dataset_type should be(None)
        }
      }
      else
        event.ctx.dataset_type should be(Some("event"))
    })

    systemEvents.foreach(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      event.etype should be(EventID.METRIC)
      event.ctx.module should be(ModuleID.processing)
      event.ctx.pdata.id should be(routerConfig.jobName)
      event.ctx.pdata.`type` should be(PDataType.flink)
      event.ctx.pdata.pid.get should be(Producer.router)
      if (event.data.error.isDefined) {
        val errorLog = event.data.error.get
        errorLog.error_level should be(ErrorLevel.critical)
        errorLog.pdata_id should be(Producer.router)
        errorLog.pdata_status should be(StatusCode.failed)
        errorLog.error_count.get should be(1)
        errorLog.error_code should be(ErrorConstants.INDEX_KEY_MISSING_OR_BLANK.errorCode)
        errorLog.error_message should be(ErrorConstants.INDEX_KEY_MISSING_OR_BLANK.errorMsg)
        errorLog.error_type should be(FunctionalError.MissingTimestampKey)
      } else {
        event.data.pipeline_stats.isDefined should be(true)
        event.data.pipeline_stats.get.latency_time.isDefined should be(true)
        event.data.pipeline_stats.get.processing_time.isDefined should be(true)
        event.data.pipeline_stats.get.total_processing_time.isDefined should be(true)
      }

    })
  }

  private def validateMetrics(metricsReporter: InMemoryReporter): Unit = {

    val d1Metrics = getMetrics(metricsReporter, "d1")
    d1Metrics(routerConfig.routerTotalCount) should be(1)
    d1Metrics(routerConfig.routerSuccessCount) should be(1)

    val d2Metrics = getMetrics(metricsReporter, "d2")
    d2Metrics(routerConfig.routerTotalCount) should be(1)
    d2Metrics(routerConfig.eventFailedMetricsCount) should be(1)

  }

}
