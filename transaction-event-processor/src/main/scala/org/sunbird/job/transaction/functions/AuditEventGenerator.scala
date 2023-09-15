package org.sunbird.job.transaction.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.{BaseProcessFunction, Metrics}

class AuditEventGenerator(config: TransactionEventProcessorConfig)
  extends BaseProcessFunction[Event, String](config) with TransactionEventProcessorService {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGenerator])

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.emptySchemaEventCount, config.emptyPropsEventCount
      , config.totalAuditEventsCount, config.skippedAuditEventsCount, config.failedAuditEventsCount, config.auditEventSuccessCount)
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
    try {
      logger.info("Metrics -> "+ metrics)
      metrics.incCounter(config.totalAuditEventsCount)
      if (event.isValid) {
        logger.info("valid event: " + event.nodeUniqueId)
        processAuditEvent(event, context, metrics)(config)
      } else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }
}
