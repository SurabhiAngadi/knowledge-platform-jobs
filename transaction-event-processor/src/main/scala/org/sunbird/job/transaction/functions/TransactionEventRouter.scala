package org.sunbird.job.transaction.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class TransactionEventRouter(config: TransactionEventProcessorConfig) extends BaseProcessFunction[Event, String](config) {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[TransactionEventRouter])
  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.emptySchemaEventCount, config.emptyPropsEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    val inputEvent = JSONUtil.serialize(event)
    try {
      metrics.incCounter(config.totalEventsCount)
      if (event.isValid) {
        logger.info("Valid event -> " + event.nodeUniqueId)
        println("Valid event -> " + event.nodeUniqueId)
        context.output(config.outputTag,inputEvent)
        }
      else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }
}

