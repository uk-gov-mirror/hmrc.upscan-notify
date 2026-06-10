/*
 * Copyright 2023 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.upscannotify.config

import play.api.{Configuration, Logging}

import javax.inject.Inject
import scala.concurrent.duration._

trait ServiceConfiguration:
  def retryInterval: FiniteDuration
  
  def maxRetryInterval: FiniteDuration

  def outboundSuccessfulQueueUrl: String

  def successfulProcessingBatchSize: Int

  def successfulWaitTime: Duration

  def outboundQuarantineQueueUrl: String

  def quarantineProcessingBatchSize: Int

  def quarantineWaitTime: Duration

  def accessKeyId: String

  def secretAccessKey: String

  def secretArn: String

  def sessionToken: Option[String]

  def awsRegion: String

  def s3UrlExpirationPeriod(serviceName: String): FiniteDuration

  def endToEndProcessingThreshold: Duration

class PlayBasedServiceConfiguration @Inject()(configuration: Configuration) extends ServiceConfiguration with Logging:
  private val defaultS3UrlExpirationPeriodKey          = "aws.s3.urlExpirationPeriod"
  private val maxS3UrlExpirationPeriod: FiniteDuration = 7.days

  override def outboundSuccessfulQueueUrl: String =
    configuration.get[String]("aws.sqs.queue.outbound.successful")

  override def successfulProcessingBatchSize: Int =
    configuration.get[Int]("successful.processingBatchSize")

  override def successfulWaitTime: Duration =
    configuration.get[Duration]("successful.waitTime")

  override def outboundQuarantineQueueUrl: String =
    configuration.get[String]("aws.sqs.queue.outbound.quarantine")

  override def quarantineProcessingBatchSize: Int =
    configuration.get[Int]("quarantine.processingBatchSize")

  override def quarantineWaitTime: Duration =
    configuration.get[Duration]("quarantine.waitTime")

  override def awsRegion: String =
    configuration.get[String]("aws.s3.region")

  override def accessKeyId: String =
    configuration.get[String]("aws.accessKeyId")

  override def secretAccessKey: String =
    configuration.get[String]("aws.secretAccessKey")

  override def secretArn: String =
    configuration.get[String]("aws.secretArn")

  override def sessionToken: Option[String] =
    configuration.getOptional[String]("aws.sessionToken")

  override def retryInterval: FiniteDuration =
    configuration.get[FiniteDuration]("aws.sqs.retry.interval")

  override def maxRetryInterval: FiniteDuration =
    configuration.get[FiniteDuration]("aws.sqs.max.retry.interval")
    
  override def s3UrlExpirationPeriod(serviceName: String): FiniteDuration =
    val serviceS3UrlExpiry = validS3UrlExpirationPeriodWithKey(configKeyForConsumingService(serviceName, defaultS3UrlExpirationPeriodKey))
    val defaultS3UrlExpiry = defaultS3UrlExpirationPeriodKey -> configuration.get[FiniteDuration](configKeyForDefault(defaultS3UrlExpirationPeriodKey))

    val (source, value) = serviceS3UrlExpiry.getOrElse(defaultS3UrlExpiry)
    logger.debug(s"Using configuration value of [$value] for s3UrlExpirationPeriod for service [$serviceName] from config [$source]")
    value

  override def endToEndProcessingThreshold: Duration =
    configuration.get[Duration]("upscan.endToEndProcessing.threshold")

  private def replaceInvalidChars(serviceName: String): String =
    serviceName.replaceAll("[/.,]", "-")

  private def validS3UrlExpirationPeriodWithKey(key: String): Option[(String, FiniteDuration)] =
    readDurationAsMillis(key).map(_.milliseconds)
      .filter(_ <= maxS3UrlExpirationPeriod)
      .map(key -> _)

  private def readDurationAsMillis(key: String): Option[Long] =
    configuration.getOptional[scala.concurrent.duration.Duration](key).map(_.toMillis)

  private def configKeyForDefault(configDescriptor: String): String =
    s"default.$configDescriptor"

  private def configKeyForConsumingService(serviceName: String, configDescriptor: String): String =
    s"consuming-services.${replaceInvalidChars(serviceName)}.$configDescriptor"
