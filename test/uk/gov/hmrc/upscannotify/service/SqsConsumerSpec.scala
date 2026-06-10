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

import org.apache.pekko.actor.ActorSystem
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when, atLeast as atLeastTimes}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse, DeleteMessageRequest, DeleteMessageResponse, Message, MessageSystemAttributeName, ReceiveMessageRequest, ReceiveMessageResponse}
import uk.gov.hmrc.upscannotify.config.ServiceConfiguration
import uk.gov.hmrc.upscannotify.model.Message as UpscanMessage
import uk.gov.hmrc.upscannotify.test.UnitSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.FutureConverters.*

class SqsConsumerSpec extends UnitSpec with Eventually with BeforeAndAfterEach:
  self =>

  var actorSystem: ActorSystem = ActorSystem()

  val serviceConfiguration = mock[ServiceConfiguration]
  when(serviceConfiguration.retryInterval)
    .thenReturn(1.second)
  when(serviceConfiguration.maxRetryInterval)
    .thenReturn(20.seconds)
  val queueUrl = "queueUrl"

  "SqsConsumer" should:
    "continuously poll the queue" in:
      given ActorSystem = actorSystem

      val callCount = AtomicInteger(0)

      val pollingJob: PollingJob =
        new PollingJob:
          override def processMessage(message: UpscanMessage): Future[Boolean] =
            callCount.incrementAndGet()
            Future.successful(true)

          override def processingBatchSize: Int =
            1
          override def queueUrl: String =
            self.queueUrl
          override def waitTime: scala.concurrent.duration.Duration =
            10.seconds

      val jobs = PollingJobs(List(pollingJob))

      val sqsClient = mock[SqsAsyncClient]
      when(sqsClient.deleteMessage(any[DeleteMessageRequest]))
        .thenReturn(Future.successful(DeleteMessageResponse.builder().build()).asJava)
      when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
        .thenReturn:
          Future
            .successful:
              ReceiveMessageResponse.builder().messages(Message.builder().build()).build()
            .asJava

      SqsConsumer(sqsClient, jobs, serviceConfiguration)

      eventually:
        callCount.get() should be > 5

      // just assert > 4 - the final one may not have been deleted yet
      verify(sqsClient, atLeastTimes(1)).deleteMessage(any[DeleteMessageRequest])


    "not delete a message if it was not handled" in:
      given ActorSystem = actorSystem

      val genId     = AtomicInteger(0)
      val callCount = AtomicInteger(0)

      val pollingJob: PollingJob =
        new PollingJob:
          override def processMessage(message: UpscanMessage): Future[Boolean] =
            callCount.incrementAndGet()
            if message.receiptHandle == "2" then
              Future.successful(false)
            else
              Future.successful(true)

          override def processingBatchSize: Int =
            1
          override def queueUrl: String =
            self.queueUrl
          override def waitTime: scala.concurrent.duration.Duration =
            10.seconds

      val jobs = PollingJobs(List(pollingJob))

      val sqsClient = mock[SqsAsyncClient]
      when(sqsClient.deleteMessage(any[DeleteMessageRequest]))
        .thenReturn(Future.successful(DeleteMessageResponse.builder().build()).asJava)
      when(sqsClient.changeMessageVisibility(any[ChangeMessageVisibilityRequest]))
        .thenReturn(Future.successful(ChangeMessageVisibilityResponse.builder().build()).asJava)
      when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
        .thenAnswer: _ =>
          Future
            .successful:
              ReceiveMessageResponse
                .builder()
                .messages:
                  Message
                    .builder()
                    .receiptHandle(genId.incrementAndGet().toString)
                    .build()
                .build()
            .asJava


      SqsConsumer(sqsClient, jobs, serviceConfiguration)

      eventually:
        callCount.get() should be > 5

      // final one may not have been deleted yet
      verify(sqsClient, atLeastTimes(3)).deleteMessage(any[DeleteMessageRequest])

      //specifically
      verify(sqsClient).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("1").build())
      verify(sqsClient).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("3").build())
      // but not
      verify(sqsClient, times(0)).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("2").build())
      // instead
      verify(sqsClient).changeMessageVisibility(ChangeMessageVisibilityRequest.builder().queueUrl(queueUrl).receiptHandle("2").visibilityTimeout(serviceConfiguration.retryInterval.toSeconds.toInt).build())

    "recover from failure" in:
      given ActorSystem = actorSystem

      val genId     = AtomicInteger(0)
      val callCount = AtomicInteger(0)

      val pollingJob: PollingJob =
        new PollingJob:
          override def processMessage(message: UpscanMessage): Future[Boolean] =
            callCount.incrementAndGet()
            if message.receiptHandle == "2" then
              Future.failed(RuntimeException("Planned failure"))
            else
              Future.successful(true)

          override def processingBatchSize: Int =
            1
          override def queueUrl: String =
            self.queueUrl
          override def waitTime: scala.concurrent.duration.Duration =
            10.seconds

      val jobs = PollingJobs(List(pollingJob))

      val sqsClient = mock[SqsAsyncClient]
      when(sqsClient.deleteMessage(any[DeleteMessageRequest]))
        .thenReturn(Future.successful(DeleteMessageResponse.builder().build()).asJava)
      when(sqsClient.changeMessageVisibility(any[ChangeMessageVisibilityRequest]))
        .thenReturn(Future.successful(ChangeMessageVisibilityResponse.builder().build()).asJava)
      when(sqsClient.receiveMessage(any[ReceiveMessageRequest]))
        .thenAnswer: _ =>
          Future
            .successful:
              ReceiveMessageResponse
                .builder()
                .messages:
                  Message
                    .builder()
                    .receiptHandle(genId.incrementAndGet().toString)
                    .build()
                .build()
            .asJava


      SqsConsumer(sqsClient, jobs, serviceConfiguration)

      eventually:
        callCount.get() should be > 5

      // final one may not have been deleted yet
      verify(sqsClient, atLeastTimes(3)).deleteMessage(any[DeleteMessageRequest])

      //specifically
      verify(sqsClient).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("1").build())
      verify(sqsClient).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("3").build())
      // but not
      verify(sqsClient, times(0)).deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle("2").build())
      // instead
      verify(sqsClient).changeMessageVisibility(ChangeMessageVisibilityRequest.builder().queueUrl(queueUrl).receiptHandle("2").visibilityTimeout(serviceConfiguration.retryInterval.toSeconds.toInt).build())

  override def beforeEach(): Unit =
    actorSystem = ActorSystem()

  override def afterEach(): Unit =
    actorSystem.terminate()
