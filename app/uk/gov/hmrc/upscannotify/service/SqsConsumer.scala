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

package uk.gov.hmrc.upscannotify.service

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import play.api.Logging
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityRequest, DeleteMessageRequest, Message, MessageSystemAttributeName, ReceiveMessageRequest}
import uk.gov.hmrc.upscannotify.config.ServiceConfiguration

import javax.inject.Inject
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

trait PollingJob:
  def processMessage(message: uk.gov.hmrc.upscannotify.model.Message): Future[Boolean]

  def jobName: String = this.getClass.getName

  def queueUrl            : String
  def processingBatchSize : Int
  def waitTime            : Duration

case class PollingJobs(
  jobs: Seq[PollingJob]
)


class SqsConsumer @Inject()(
  sqsClient           : SqsAsyncClient,
  pollingJobs         : PollingJobs,
  serviceConfiguration: ServiceConfiguration
)(using
  actorSystem         : ActorSystem,
  ec                  : ExecutionContext
) extends Logging:

  def runQueue(job: PollingJob): Future[Done] =
    Source
      .repeat:
        ReceiveMessageRequest.builder()
          .queueUrl(job.queueUrl)
          .maxNumberOfMessages(job.processingBatchSize)
          .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
          .waitTimeSeconds(job.waitTime.toSeconds.toInt)
          .build
      .mapAsync(parallelism = 1)(getMessages(job.queueUrl, _))
      .mapConcat(identity)
      .mapAsync(parallelism = 1): message =>
        job.processMessage(uk.gov.hmrc.upscannotify.model.Message(message.messageId, message.body, message.receiptHandle, receivedAt = Instant.now()))
          .flatMap: isHandled =>
            if isHandled then
              deleteMessage(job.queueUrl, message)
            else
            // message will return to queue after retryInterval
            // Note, we previously stopped processing *all* messages on this instance until the retryInterval
            // We probably only need to do this for exceptions that are known to affect all messages
            // This could be done by completing Future.unit after a timeout (e.g. complete a promise with `context.system.scheduler.scheduleOnce`)
            returnMessage(job.queueUrl, message)
        .recover:
          case NonFatal(e) =>
            logger.error(s"Failed to process message ${message.messageId} for job [${job.jobName}]", e)
            returnMessage(job.queueUrl, message)
      .run()
      .andThen: res =>
        logger.info(s"Queue for job [${job.jobName}] terminated: $res - restarting")
        actorSystem.scheduler.scheduleOnce(10.seconds)(runQueue(job))

  pollingJobs.jobs
    .foreach(runQueue)

  private def getMessages(queueUrl: String, req: ReceiveMessageRequest): Future[Seq[Message]] =
    logger.info(s"receiving messages from Queue: [${queueUrl}]")
    sqsClient.receiveMessage(req)
      .asScala
      .map(_.messages.asScala.toSeq)
      .map: res =>
        logger.info(s"received ${res.size} messages")
        res

  private def deleteMessage(queueUrl: String, message: Message): Future[Unit] =
    sqsClient
      .deleteMessage:
        DeleteMessageRequest.builder()
          .queueUrl(queueUrl)
          .receiptHandle(message.receiptHandle)
          .build()
      .asScala
      .map: _ =>
        logger.debug:
          s"Deleted message from Queue: [${queueUrl}], for receiptHandle: [${message.receiptHandle}]."

  private def receiveCount(message: Message): Int =
    Option(message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT))
      .flatMap(_.toIntOption)
      .getOrElse(1)

  private def retryDelaySeconds(retryCount: Int): Int =
    math.min(
      serviceConfiguration.retryInterval.toSeconds.toInt *
        math.pow(2, retryCount - 1).toInt,
      serviceConfiguration.maxRetryInterval.toSeconds.toInt
    )

  private def returnMessage(queueUrl: String, message: Message): Future[Unit] =
    val retryCount = receiveCount(message)
    val delay      = retryDelaySeconds(retryCount)

    sqsClient
      .changeMessageVisibility:
        ChangeMessageVisibilityRequest.builder()
          .queueUrl(queueUrl)
          .receiptHandle(message.receiptHandle)
          .visibilityTimeout(delay)
          .build()
      .asScala
      .map: _ =>
        logger.debug:
          s"Returned message back to the queue (attempt $retryCount, delay $delay sec): [${queueUrl}], for receiptHandle: [${message.receiptHandle}]."
