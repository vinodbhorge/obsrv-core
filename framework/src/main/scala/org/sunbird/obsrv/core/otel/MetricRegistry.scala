package org.sunbird.obsrv.core.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.{LongCounter, Meter}
import com.typesafe.config.Config

object MetricRegistry {
  private val config: Config = OTelService.getConfig
  private val oTel: Option[OpenTelemetry] =
    if (config.getBoolean("otel.enable")) OTelService.init()
    else None

  private val meter: Option[Meter] = oTel.map(_.meterBuilder("obsrv-pipeline").build())

  // Helper method to register a metric
  private def register(name: String, description: String, unit: String): Option[LongCounter] = {
    meter.map(_.counterBuilder(name)
      .setDescription(description)
      .setUnit(unit)
      .build())
  }

  // Metric definitions using the helper method
  val errorCount: Option[LongCounter] = register("event.error.count", "Dataset Error Event Count", "1")
  val processingTimeCounter: Option[LongCounter] = register("pipeline.processing.time", "Processing Time", "ms")
  val totalProcessingTimeCounter: Option[LongCounter] = register("pipeline.total.processing.time", "Total Processing Time", "ms")
  val latencyTimeCounter: Option[LongCounter] = register("pipeline.latency.time", "Latency Time", "ms")
  val extractorEventCounter: Option[LongCounter] = register("pipeline.extractor.events.count", "Count of Extractor Events", "1")
  val extractorTimeCounter: Option[LongCounter] = register("pipeline.extractor.time", "Extractor Processing Time", "ms")
  val transformStatusCounter: Option[LongCounter] = register("pipeline.transform.status", "Data Transform Status", "1")
  val transformTimeCounter: Option[LongCounter] = register("pipeline.transform.time", "Transformation Processing Time", "ms")
  val denormStatusCounter: Option[LongCounter] = register("pipeline.denorm.status", "Denormalization Status", "1")
  val denormTimeCounter: Option[LongCounter] = register("pipeline.denorm.time", "Denormalization Processing Time", "ms")
  val dedupStatusCounter: Option[LongCounter] = register("pipeline.dedup.status", "Deduplication Status", "1")
  val dedupTimeCounter: Option[LongCounter] = register("pipeline.dedup.time", "Deduplication Processing Time", "ms")
  val validatorTimeCounter: Option[LongCounter] = register("pipeline.validator.time", "Validator Processing Time", "ms")
  val validatorStatusCounter: Option[LongCounter] = register("pipeline.validator.status", "Validator Status", "1")
  val extractorStatusCounter: Option[LongCounter] = register("pipeline.extractor.status", "Extractor Status", "1")
}
