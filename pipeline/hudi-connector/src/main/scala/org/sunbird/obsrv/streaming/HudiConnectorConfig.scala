package org.sunbird.obsrv.streaming

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.hudi.common.model.HoodieTableType
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class HudiConnectorConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "Flink-Hudi-Connector") {

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  override def inputTopic(): String = config.getString("kafka.input.topic")

  val kafkaDefaultOutputTopic: String = config.getString("kafka.output.topic")

  override def inputConsumer(): String = config.getString("kafka.groupId")

  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("dummy-events")

  override def failedEventsOutputTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")

  val kafkaInvalidTopic: String = config.getString("kafka.output.invalid.topic")

  val invalidEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("invalid-events")
  val validEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("valid-events")

  val invalidEventProducer = "invalid-events-sink"


  val hudiTableType: String =
    if (config.getString("hudi.table.type").equalsIgnoreCase("MERGE_ON_READ"))
      HoodieTableType.MERGE_ON_READ.name()
    else if (config.getString("hudi.table.type").equalsIgnoreCase("COPY_ON_WRITE"))
      HoodieTableType.COPY_ON_WRITE.name()
    else HoodieTableType.MERGE_ON_READ.name()

  val hudiBasePath: String = config.getString("hudi.table.base.path")

  val hmsEnabled: Boolean = if (config.hasPath("hudi.hms.enabled")) config.getBoolean("hudi.hms.enabled") else false
  val hmsUsername: String = config.getString("hudi.hms.database.username")
  val hmsPassword: String = config.getString("hudi.hms.database.password")
  val hmsDatabaseName: String = config.getString("hudi.hms.database.name")
  val hmsURI: String = config.getString("hudi.hms.uri")

  val hudiWriteTasks: Int = config.getInt("hudi.write.tasks")
  val hudiCompactionTasks: Int = config.getInt("hudi.compaction.tasks")
  val hudiWriteBatchSize: Int = config.getInt("hudi.write.batch.size")
  val deltaCommits: Int = config.getInt("hudi.delta.commits")
  val compactionDeltaSeconds: Int = config.getInt("hudi.delta.seconds")
  val compressionCodec: String = config.getString("hudi.compression.codec")
  val hudiCompactionEnabled: Boolean = config.getBoolean("hudi.compaction.enabled")
  val hudiMetadataEnabled: Boolean = config.getBoolean("hudi.metadata.enabled")
  val hudiIndexType: String = config.getString("hudi.index.type")

  // Memory
  val hudiWriteTaskMemory: Int = config.getInt("hudi.write.task.max.memory")
  val hudiCompactionTaskMemory: Int = config.getInt("hudi.write.compaction.max.memory")
  val hudiFsAtomicCreationSupport: String = config.getString("hudi.fs.atomic_creation.support")

  // Metrics

  val inputEventCountMetric = "input-event-count"
  val failedEventCountMetric = "failed-event-count"

  // Metrics Exporter
  val metricsReportType: String =  config.getString("metrics.reporter.type")
  val metricsReporterHost: String = config.getString("metrics.reporter.host")
  val metricsReporterPort: String = config.getString("metrics.reporter.port")


}
