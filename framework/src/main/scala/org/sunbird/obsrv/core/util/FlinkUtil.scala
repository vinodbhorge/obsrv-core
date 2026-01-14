package org.sunbird.obsrv.core.util

import org.apache.flink.configuration._
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import java.time.Duration

object FlinkUtil {

  def getExecutionContext(config: BaseJobConfig[_]): StreamExecutionEnvironment = {

    val configuration = new Configuration()
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay")
    configuration.set[Integer](RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, config.restartAttempts)
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(config.delayBetweenAttempts))
    configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(config.checkpointingInterval))
    configuration.set[java.lang.Boolean](ExecutionOptions.SNAPSHOT_COMPRESSION, config.enableCompressedCheckpointing)

    // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked with a cloud blob store config
    /* Use Blob storage as distributed state backend if enabled */
    config.enableDistributedCheckpointing match {
      case Some(true) => {
        configuration.set(StateBackendOptions.STATE_BACKEND, "hashmap")
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE)
        configuration.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(config.checkpointingPauseSeconds))
        configuration.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem")
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, s"${config.checkpointingBaseUrl.getOrElse("")}/${config.jobName}")
      }
      case _ => // Do nothing
    }
    // $COVERAGE-ON$
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    print("Flink Config:", env.getConfiguration)
    env
  }
}