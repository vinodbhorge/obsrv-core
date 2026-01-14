package org.sunbird.obsrv.util


import org.apache.flink.metrics.Gauge

import scala.collection.concurrent.TrieMap

class HMetrics {
  private val metricStore = TrieMap[(String, String), Long]()

  def increment(dataset: String, metric: String, value: Long): Unit = {
    metricStore.synchronized {
      val key = (dataset, metric)
      val current = metricStore.getOrElse(key, 0L)
      metricStore.put(key, current + value)
    }
  }

  def getAndReset(dataset: String, metric: String): Long = {
    metricStore.synchronized {
      val key = (dataset, metric)
      val current = metricStore.getOrElse(key, 0L)
      metricStore.remove(key)
      current
    }
  }
}

case class ScalaGauge[T](getValueFn: () => T) extends Gauge[T] {
  override def getValue: T = getValueFn()
}
