package org.sunbird.obsrv.preprocessor

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetConfig, IndexingConfig, KeysConfig, RouterConfig}
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.preprocessor.fixture.EventFixtures
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.preprocessor.util.SchemaValidator

class TestSchemaValidator extends FlatSpec with Matchers {

  val config: Config = ConfigFactory.load("test.conf")
  val pipelineProcessorConfig = new PipelinePreprocessorConfig(config)
  val schemaValidator = new SchemaValidator()

  "SchemaValidator" should "return a success report for a valid event" in {

    val dataset = Dataset("d1", "dataset", None, None, None, Option(EventFixtures.VALID_SCHEMA), None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")
    schemaValidator.loadDataSchema(dataset)

    val event = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.VALID_SCHEMA_EVENT)
    val report = schemaValidator.validate("d1", event)
    assert(report.isEmpty)
  }

  it should "return a failed validation report for a invalid event" in {

    val dataset = Dataset("d1", "dataset", None, None, None, Option(EventFixtures.VALID_SCHEMA), None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")
    schemaValidator.loadDataSchema(dataset)

    val event1 = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.INVALID_SCHEMA_EVENT)
    val report1 = schemaValidator.validate("d1", event1)
    val messages1 = schemaValidator.getValidationMessages(report1)
    assert(!report1.isEmpty)
    assert(messages1.size == 1)
    messages1.head.message should be(": required property 'vehicleCode' not found")
    messages1.head.messageKey.get should be("required")
    messages1.head.property.get should be ("vehicleCode")

    val event2 = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.INVALID_SCHEMA_EVENT2)
    val report2 = schemaValidator.validate("d1", event2)
    val messages2 = schemaValidator.getValidationMessages(report2)
    assert(!report2.isEmpty)
    assert(messages2.size == 2)
    messages2.foreach(f => {
      f.arguments.getOrElse(Seq("")).head match {
        case "integer" =>
          f.message should be("/id: integer found, string expected")
          f.instanceLocation.get should be("/id")
        case "array" =>
          f.message should be("/vehicleCode: array found, string expected")
          f.instanceLocation.get should be ("/vehicleCode")
      }
    })

    val event3 = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.INVALID_SCHEMA_EVENT3)
    val report3 = schemaValidator.validate("d1", event3)
    val messages3 = schemaValidator.getValidationMessages(report3)
    assert(!report3.isEmpty)
    assert(messages3.size == 2)
    messages3.foreach(f => {
      f.messageKey.get match {
        case "type" =>
          f.message should be("/id: integer found, string expected")
          f.instanceLocation.get should be("/id")
          f.arguments.getOrElse(Seq("")).head should be ("integer")
          f.arguments.getOrElse(Seq("")).tail.head should be("string")
        case "additionalProperties" =>
          f.message should be("/metrics: property 'deliveriesRejected' is not defined in the schema and the schema does not allow additional properties")
          f.instanceLocation.get should be("/metrics")
          f.arguments.getOrElse(Seq("")).head should be("deliveriesRejected")
      }
    })
  }

  it should "validate the negative and missing scenarios" in {
    val dataset = Dataset("d4", "dataset", None, None, None, Option(EventFixtures.INVALID_SCHEMA_JSON), None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")
    schemaValidator.loadDataSchema(dataset)
    schemaValidator.schemaFileExists(dataset) should be(false)

    schemaValidator.loadDataSchema(dataset)
    schemaValidator.schemaFileExists(dataset) should be(false)

    val dataset2 = Dataset("d5", "dataset", None, None, None, None, None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")
    schemaValidator.loadDataSchemas(List[Dataset](dataset2))
    schemaValidator.schemaFileExists(dataset2) should be(false)

    val dataset3 = Dataset("d6", "dataset", None, None, None, Option(EventFixtures.INVALID_SCHEMA), None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")

    schemaValidator.loadDataSchemas(List[Dataset](dataset3))
    schemaValidator.schemaFileExists(dataset3) should be(false)

    val dataset4 = Dataset("d7", "dataset", None, None, None, Option(EventFixtures.INVALID_SCHEMA), None, RouterConfig(""), DatasetConfig(IndexingConfig(olapStoreEnabled = false, lakehouseEnabled = false, cacheEnabled = false), KeysConfig(Some("id"), None, Some("date"), None)), DatasetStatus.Live, "ingest")
    schemaValidator.schemaFileExists(dataset4) should be(false)
  }

}
