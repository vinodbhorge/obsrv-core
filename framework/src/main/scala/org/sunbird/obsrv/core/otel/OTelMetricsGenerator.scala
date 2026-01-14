package org.sunbird.obsrv.core.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{LongCounter, Meter}
import org.sunbird.obsrv.core.model.Models.SystemEvent
import com.typesafe.config.{Config, ConfigFactory}

object OTelMetricsGenerator {

  private val config: Config = OTelService.getConfig
  private val oTel: Option[OpenTelemetry] = if (config.getBoolean("otel.enable")) OTelService.init() else None

  def generateOTelSystemEventMetric(systemEvent: SystemEvent): Unit = {
    oTel match {
      case Some(openTelemetry) =>
        generate(systemEvent, openTelemetry)
      case None =>
      // Do nothing.
      //println("OpenTelemetry is disabled. No metrics generated.")
    }
  }

  def generate(systemEvent: SystemEvent, openTelemetry: OpenTelemetry): Unit = {
    val meter: Meter = openTelemetry.meterBuilder("obsrv-pipeline").build()
    val errorCount: LongCounter = meter.counterBuilder("event.error.count")
      .setDescription("Dataset Error Event Count")
      .setUnit("1")
      .build()

    val contextAttributes: Attributes = Attributes.builder()
      .put("ctx.pdata.id", systemEvent.ctx.pdata.id)
      .put("ctx.pdata.type", systemEvent.ctx.pdata.`type`.toString)
      .put("ctx.pdata.pid", systemEvent.ctx.pdata.pid.getOrElse("unknown").toString)
      .put("ctx.module", systemEvent.ctx.module.toString)
      .put("ctx.dataset", systemEvent.ctx.dataset.getOrElse("unknown"))
      .put("ctx.dataset_type", systemEvent.ctx.dataset_type.getOrElse("unknown"))
      .build()

    // Handle error events and add them to the error count
    systemEvent.data.error.foreach { errorLog =>
      val errorAttributes: Attributes = Attributes.builder()
        .put("error.pdata_id", errorLog.pdata_id.toString)
        .put("error.pdata_status", errorLog.pdata_status.toString)
        .put("error.error_type", errorLog.error_type.toString)
        .put("error.error_code", errorLog.error_code)
        .put("error.error_message", errorLog.error_message)
        .put("error.error_level", errorLog.error_level.toString)
        .put("error.error_count", errorLog.error_count.getOrElse(0).toLong)
        .putAll(contextAttributes)
        .build()

      errorCount.add(1, errorAttributes)
    }

    // Handle pipeline stats
    systemEvent.data.pipeline_stats.foreach { stats =>
      // Extractor Job Metrics
      stats.extractor_events.foreach { events =>
        MetricRegistry.extractorEventCounter.foreach { counter =>
          counter.add(events.toLong, contextAttributes) // Correct way to add to counter
        }
      }

      stats.extractor_time.foreach { time =>
        MetricRegistry.extractorTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      stats.extractor_status.foreach { status =>
        MetricRegistry.extractorStatusCounter.foreach { counter =>
          counter.add(1, Attributes.builder().put("status", status.toString).putAll(contextAttributes).build())
        }
      }

      // Schema Validator Metrics
      stats.validator_status.foreach { status =>
        MetricRegistry.validatorStatusCounter.foreach { counter =>
          counter.add(1, Attributes.builder().put("status", status.toString).putAll(contextAttributes).build())
        }
      }
      stats.validator_time.foreach { time =>
        MetricRegistry.validatorTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      // De-Duplication Metrics
      stats.dedup_time.foreach { time =>
        MetricRegistry.dedupTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      stats.dedup_status.foreach { status =>
        MetricRegistry.dedupStatusCounter.foreach { counter =>
          counter.add(1, Attributes.builder().put("status", status.toString).putAll(contextAttributes).build())
        }
      }

      // De-normalisation Metrics
      stats.denorm_time.foreach { time =>
        MetricRegistry.denormTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      stats.denorm_status.foreach { status =>
        MetricRegistry.denormStatusCounter.foreach { counter =>
          counter.add(1, Attributes.builder().put("status", status.toString).putAll(contextAttributes).build())
        }
      }

      // Data transformation Metrics
      stats.transform_time.foreach { time =>
        MetricRegistry.transformTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }
      stats.transform_status.foreach { status =>
        MetricRegistry.transformStatusCounter.foreach { counter =>
          counter.add(1, Attributes.builder().put("status", status.toString).putAll(contextAttributes).build())
        }
      }

      // Common timestamp Metrics
      stats.total_processing_time.foreach { time =>
        MetricRegistry.totalProcessingTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      stats.latency_time.foreach { time =>
        MetricRegistry.latencyTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }

      stats.processing_time.foreach { time =>
        MetricRegistry.processingTimeCounter.foreach { counter =>
          counter.add(time, contextAttributes)
        }
      }
    }
  }
}
