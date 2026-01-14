package org.sunbird.obsrv.core.otel

import com.typesafe.config.{Config, ConfigFactory}
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.logs.SdkLoggerProvider
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.semconv.ResourceAttributes

import java.io.File
import java.util.concurrent.TimeUnit

object OTelService {
  private val configFile = new File("/data/flink/conf/baseconfig.conf")
  // $COVERAGE-OFF$ This code only executes within a flink cluster
  val config: Config = if (configFile.exists()) {
    println("Loading configuration file cluster  baseconfig.conf...")
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    println("Loading configuration file baseconfig.conf inside the jar...")
    ConfigFactory.load("baseconfig.conf").withFallback(ConfigFactory.systemEnvironment())
  }

  def init(): Option[OpenTelemetry] = {
    val collectorEndpoint: String = config.getString("otel.collector.endpoint")
    val enable: Boolean = config.getBoolean("otel.enable")
    if (enable) {
      val tracerProvider = createTracerProvider()
      val meterProvider = createMeterProvider(createOtlpMetricExporter(collectorEndpoint))
      val loggerProvider = createLoggerProvider(collectorEndpoint)

      // Build the OpenTelemetry SDK
      val openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setMeterProvider(meterProvider)
        .setLoggerProvider(loggerProvider)
        .build()

      sys.addShutdownHook(openTelemetry.close())
      println("Open Telemetry is Enabled")
      Some(openTelemetry)
    } else {
      println("Open Telemetry is Disabled")
      None // Return None if OpenTelemetry is not enabled
    }
  }


  private def createOtlpMetricExporter(endpoint: String): OtlpGrpcMetricExporter = {
    OtlpGrpcMetricExporter.builder()
      .setEndpoint(endpoint)
      .setTimeout(5, TimeUnit.SECONDS)
      .build()
  }

  private def createTracerProvider(): SdkTracerProvider = {
    SdkTracerProvider.builder()
      .setSampler(Sampler.alwaysOn())
      .build()
  }

  private def createMeterProvider(metricExporter: OtlpGrpcMetricExporter): SdkMeterProvider = {
    SdkMeterProvider.builder()
      .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
      .setResource(createServiceResource("obsrv-pipeline"))
      .build()
  }

  private def createLoggerProvider(endpoint: String): SdkLoggerProvider = {
    SdkLoggerProvider.builder()
      .setResource(createServiceResource("obsrv-pipeline"))
      .addLogRecordProcessor(
        BatchLogRecordProcessor.builder(
          OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(endpoint)
            .build()
        ).build()
      )
      .build()
  }

  // Helper method to create a Resource with service name
  private def createServiceResource(serviceName: String): Resource = {
    Resource.getDefault().toBuilder()
      .put(ResourceAttributes.SERVICE_NAME, serviceName)
      .build()
  }

  def getConfig: Config = {
    this.config
  }
}
