package org.sunbird.obsrv.pipeline

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.{InMemoryReporter, MiniClusterResourceConfiguration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.pipeline.task.{UnifiedPipelineConfig, UnifiedPipelineStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.duration._

class UnifiedPipelineStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  private val metricsReporter = InMemoryReporter.createWithRetainedMetrics

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(metricsReporter.addToConfiguration(new Configuration()))
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val unifiedPipelineConfig = new UnifiedPipelineConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(unifiedPipelineConfig)
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
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D1)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.MISSING_DATASET_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.UNREGISTERED_DATASET_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DUPLICATE_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_INCORRECT_EXTRACTION_KEY)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_EXTRACTION_KEY_NOT_ARRAY)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D2)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_D2)
    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(unifiedPipelineConfig.redisHost, unifiedPipelineConfig.redisPort, unifiedPipelineConfig.redisConnectionTimeout)
    redisConnection.getConnection(config.getInt("redis.database.extractor.duplication.store.id")).flushAll()
    redisConnection.getConnection(config.getInt("redis.database.preprocessor.duplication.store.id")).flushAll()
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.output.system.event.topic"), config.getString("kafka.output.transform.topic"), config.getString("kafka.output.denorm.failed.topic"),
      config.getString("kafka.output.denorm.topic"), config.getString("kafka.output.duplicate.topic"), config.getString("kafka.output.unique.topic"),
      config.getString("kafka.output.invalid.topic"), config.getString("kafka.output.batch.failed.topic"), config.getString("kafka.output.failed.topic"),
      config.getString("kafka.output.extractor.duplicate.topic"), config.getString("kafka.output.raw.topic"), config.getString("kafka.input.topic"),
      "d1-events", "d2-events"
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "UnifiedPipelineStreamTaskTestSpec" should "validate the entire pipeline" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(unifiedPipelineConfig)
    val task = new UnifiedPipelineStreamTask(config, unifiedPipelineConfig, kafkaConnector)
    task.process(env)
    env.executeAsync(unifiedPipelineConfig.jobName)

    try {
      val d1Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("d1-events", 1, timeout = 30.seconds)
      d1Events.size should be(1)
      val d2Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("d2-events", 1, timeout = 30.seconds)
      d2Events.size should be(1)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    try {
      val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.output.system.event.topic"), 7, timeout = 30.seconds)
      systemEvents.size should be(7)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }

    val d1Metrics = getMetrics(metricsReporter, "d1")
    d1Metrics("extractor-total-count") should be(4)
    d1Metrics("extractor-duplicate-count") should be(1)
    d1Metrics("extractor-event-count") should be(1)
    d1Metrics("extractor-success-count") should be(1)
    d1Metrics("extractor-failed-count") should be(2)
    d1Metrics("validator-total-count") should be(1)
    d1Metrics("validator-success-count") should be(1)
    d1Metrics("dedup-total-count") should be(1)
    d1Metrics("dedup-success-count") should be(1)
    d1Metrics("denorm-total") should be(1)
    d1Metrics("denorm-failed") should be(1)
    d1Metrics("transform-total-count") should be(1)
    d1Metrics("transform-success-count") should be(1)
    d1Metrics("router-total-count") should be(1)
    d1Metrics("router-success-count") should be(1)

    val d2Metrics = getMetrics(metricsReporter, "d2")
    d2Metrics("extractor-total-count") should be(2)
    d2Metrics("failed-event-count") should be(1)
    d2Metrics("extractor-skipped-count") should be(1)
    d2Metrics("validator-total-count") should be(1)
    d2Metrics("validator-skipped-count") should be(1)
    d2Metrics("dedup-total-count") should be(1)
    d2Metrics("dedup-skipped-count") should be(1)
    d2Metrics("denorm-total") should be(1)
    d2Metrics("denorm-skipped") should be(1)
    d2Metrics("transform-total-count") should be(1)
    d2Metrics("transform-skipped-count") should be(1)
    d2Metrics("router-total-count") should be(1)
    d2Metrics("router-success-count") should be(1)

    unifiedPipelineConfig.successTag().getId should be("processing_stats")
    unifiedPipelineConfig.failedEventsOutputTag().getId should be("failed-events")
  }

}
