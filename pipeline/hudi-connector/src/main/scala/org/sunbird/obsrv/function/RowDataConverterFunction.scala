package org.sunbird.obsrv.function

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.JsonToRowDataConverters
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.table.data.RowData
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.streaming.HudiConnectorConfig
import org.sunbird.obsrv.util.{HMetrics, HudiSchemaParser, ScalaGauge}

import scala.collection.mutable.{Map => MMap}

class RowDataConverterFunction(config: HudiConnectorConfig, datasetId: String)
  extends RichMapFunction[MMap[String, AnyRef], RowData] {

  private val logger = LoggerFactory.getLogger(classOf[RowDataConverterFunction])

  private var metrics: HMetrics = _
  private var jsonToRowDataConverters: JsonToRowDataConverters = _
  private var objectMapper: ObjectMapper = _
  private var hudiSchemaParser: HudiSchemaParser = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    metrics = new HMetrics()
    jsonToRowDataConverters = new JsonToRowDataConverters(false, true, TimestampFormat.SQL)
    objectMapper = new ObjectMapper()
    hudiSchemaParser = new HudiSchemaParser()

    getRuntimeContext.getMetricGroup
      .addGroup(config.jobName)
      .addGroup(datasetId)
      .gauge[Long, ScalaGauge[Long]](config.inputEventCountMetric, ScalaGauge[Long](() =>
        metrics.getAndReset(datasetId, config.inputEventCountMetric)
      ))

    getRuntimeContext.getMetricGroup
      .addGroup(config.jobName)
      .addGroup(datasetId)
      .gauge[Long, ScalaGauge[Long]](config.failedEventCountMetric, ScalaGauge[Long](() =>
        metrics.getAndReset(datasetId, config.failedEventCountMetric)
      ))
  }

  override def map(event: MMap[String, AnyRef]): RowData = {
    try {
      if (event.nonEmpty) {
        metrics.increment(datasetId, config.inputEventCountMetric, 1)
      }
      val rowData = convertToRowData(event)
      rowData
    } catch {
      case ex: Exception =>
        metrics.increment(datasetId, config.failedEventCountMetric, 1)
        logger.error("Failed to process record", ex)
        throw ex
    }
  }

  def convertToRowData(data: MMap[String, AnyRef]): RowData = {
    val eventJson = JSONUtil.serialize(data)
    val flattenedData = hudiSchemaParser.parseJson(datasetId, eventJson)
    val rowType = hudiSchemaParser.rowTypeMap(datasetId)
    val converter: JsonToRowDataConverters.JsonToRowDataConverter =
      jsonToRowDataConverters.createRowConverter(rowType)
    converter.convert(objectMapper.readTree(JSONUtil.serialize(flattenedData))).asInstanceOf[RowData]
  }
}