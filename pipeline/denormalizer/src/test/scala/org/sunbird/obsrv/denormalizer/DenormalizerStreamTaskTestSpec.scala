package org.sunbird.obsrv.denormalizer

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.{InMemoryReporter, MiniClusterResourceConfiguration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.model.Models.SystemEvent
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.denormalizer.task.{DenormalizerConfig, DenormalizerStreamTask}
import org.sunbird.obsrv.denormalizer.util.DenormCache
import org.sunbird.obsrv.model.DatasetModels._
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.duration._

class DenormalizerStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  private val metricsReporter = InMemoryReporter.createWithRetainedMetrics
  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(metricsReporter.addToConfiguration(new Configuration()))
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val denormConfig = new DenormalizerConfig(config)
  val redisPort: Int = denormConfig.redisPort
  val kafkaConnector = new FlinkKafkaConnector(denormConfig)
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
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.SUCCESS_DENORM)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.SKIP_DENORM)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DENORM_MISSING_KEYS)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DENORM_MISSING_DATA_AND_INVALIDKEY)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.EMPTY_DENORM_FIELDS)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.EMPTY_DRNORM_CONFIG_OBJECT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DRNORM_FIELDS_KEY_NOT_PRESENT)
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, extraction_config, dedup_config, router_config, dataset_config, status, data_version, api_version, entry_topic, created_by, updated_by, created_date, updated_date) values ('d4', 'event', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"$id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":" + config.getInt("redis.port") + ",\"redis_db\":2}', 'Live', 2, 'v1', 'ingest', 'System', 'System', now(), now());")
    postgresConnect.execute("update datasets set denorm_config = '{\"redis_db_host\":\"localhost\",\"redis_db_port\":" + config.getInt("redis.port") + ",\"denorm_fields\":[]}' where id='d4';")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, extraction_config, dedup_config, router_config, dataset_config, status, data_version, api_version, entry_topic, created_by, updated_by, created_date, updated_date) values ('d5', 'event', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"$id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":" + config.getInt("redis.port") + ",\"redis_db\":2}', 'Live', 2, 'v1', 'ingest', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, extraction_config, dedup_config, router_config, dataset_config, status, data_version, api_version, entry_topic, created_by, updated_by, created_date, updated_date) values ('d6', 'event', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"$id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":" + config.getInt("redis.port") + ",\"redis_db\":2}', 'Live', 2, 'v1', 'ingest', 'System', 'System', now(), now());")
    postgresConnect.execute("update datasets set denorm_config = '{\"redis_db_host\":\"localhost\",\"redis_db_port\":" + config.getInt("redis.port") + "}' where id='d6';")
    postgresConnect.execute("update datasets set denorm_config = '" + s"""{"redis_db_host":"localhost","redis_db_port":$redisPort,"denorm_fields":[{"denorm_key":"vehicleCode","redis_db":3,"denorm_out_field":"vehicle_data"},{"jsonata_expr":"$$.dealer.dealerCode","redis_db":4,"denorm_out_field":"dealer_data"}]}""" + "' where id='d1';")
    val redisConnection = new RedisConnect(denormConfig.redisHost, denormConfig.redisPort, denormConfig.redisConnectionTimeout)
    redisConnection.getConnection(3).set("HYUN-CRE-D6", EventFixture.DENORM_DATA_1)
    redisConnection.getConnection(4).set("D123", EventFixture.DENORM_DATA_2)
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(denormConfig.redisHost, denormConfig.redisPort, denormConfig.redisConnectionTimeout)
    redisConnection.getConnection(3).flushAll()
    redisConnection.getConnection(4).flushAll()

    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.output.system.event.topic"), config.getString("kafka.output.denorm.topic"), config.getString("kafka.input.topic")
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "DenormalizerStreamTaskTestSpec" should "validate the denorm stream task" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(denormConfig)
    val task = new DenormalizerStreamTask(denormConfig, kafkaConnector)
    task.process(env)
    env.executeAsync(denormConfig.jobName)

    val outputs = EmbeddedKafka.consumeNumberMessagesFrom[String](denormConfig.denormOutputTopic, 4, timeout = 30.seconds)
    validateOutputs(outputs)

    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](denormConfig.kafkaSystemTopic, 3, timeout = 30.seconds)
    validateSystemEvents(systemEvents)

    validateMetrics(metricsReporter)
  }

  it should "validate dynamic cache creation within DenormCache" in {
    val denormCache = new DenormCache(denormConfig)
    noException should be thrownBy {
      denormCache.open(Dataset(id = "d123", datasetType = "dataset", extractionConfig = None, dedupConfig = None, validationConfig = None, jsonSchema = None,
        denormConfig = Some(DenormConfig(redisDBHost = "localhost", redisDBPort = redisPort, denormFields = List(DenormFieldConfig(denormKey = Some("vehicleCode"), redisDB = 3, denormOutField = "vehicle_data", jsonAtaExpr = None)))), routerConfig = RouterConfig(""),
        datasetConfig = DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), status = DatasetStatus.Live, "ingest"))
    }
  }

  private def validateOutputs(outputs: List[String]): Unit = {
    outputs.size should be(4)
    outputs.zipWithIndex.foreach {
      case (elem, idx) =>
        val msg = JSONUtil.deserialize[Map[String, AnyRef]](elem)
        val event = JSONUtil.serialize(msg(Constants.EVENT))
        idx match {
          case 0 => event should be("""{"vehicle_data":{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"},"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","dealer_data":{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 1 => event should be("""{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 2 => event should be("""{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"id":"2345","date":"2023-03-01","dealer_data":{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 3 => event should be("""{"dealer":{"dealerCode":"D124","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":["HYUN-CRE-D7"],"id":"4567","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 4 => event should be("""{"vehicle_data":{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"},"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","dealer_data":{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 5 => event should be("""{"vehicle_data":{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"},"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"12342","date":"2023-03-01","dealer_data":{"code":"D1231","name":"KUN United","licenseNumber":"12341241","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 6 => event should be("""{"vehicle_data":{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"},"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234211","date":"2023-03-01","dealer_data":{"code":"D12312","name":"KUN United","licenseNumber":"12341241","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
        }
    }
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(3)
    systemEvents.foreach(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      event.etype should be(EventID.METRIC)
      event.ctx.module should be(ModuleID.processing)
      event.ctx.pdata.id should be(denormConfig.jobName)
      event.ctx.pdata.`type` should be(PDataType.flink)
      event.ctx.pdata.pid.get should be(Producer.denorm)
      event.data.error.isDefined should be(true)
      val errorLog = event.data.error.get
      errorLog.error_level should be(ErrorLevel.critical)
      errorLog.pdata_id should be(Producer.denorm)
      errorLog.pdata_status should be(StatusCode.failed)
      errorLog.error_count.get should be(1)
      errorLog.error_code match {
        case ErrorConstants.DENORM_KEY_MISSING.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormKeyMissing)
        case ErrorConstants.DENORM_KEY_NOT_A_STRING_OR_NUMBER.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormKeyInvalid)
        case ErrorConstants.DENORM_DATA_NOT_FOUND.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormDataNotFound)
      }
    })
  }

  private def validateMetrics(metricsReporter: InMemoryReporter): Unit = {
    val d1Metrics = getMetrics(metricsReporter, "d1")
    d1Metrics(denormConfig.denormTotal) should be(3)
    d1Metrics(denormConfig.denormFailed) should be(1)
    d1Metrics(denormConfig.denormSuccess) should be(1)
    d1Metrics(denormConfig.denormPartialSuccess) should be(1)

    val d2Metrics = getMetrics(metricsReporter, "d2")
    d2Metrics(denormConfig.denormTotal) should be(1)
    d2Metrics(denormConfig.eventsSkipped) should be(1)

    // when denorm_fields not configured status will be skipped
    val d4Metrics = getMetrics(metricsReporter, "d4")
    d4Metrics(denormConfig.eventsSkipped) should be(1)

    // when denorm_config is empty object
    val d5Metrics = getMetrics(metricsReporter, "d5")
    d5Metrics(denormConfig.eventsSkipped) should be(1)

    // when denorm_fields key is not present
    val d6Metrics = getMetrics(metricsReporter, "d6")
    d6Metrics(denormConfig.eventsSkipped) should be(1)
  }
}
