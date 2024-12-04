/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._

import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics {

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (ApiKeys.enabledApis.asScala.toSeq.map(_.name) ++
        Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String) = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                memoryPool: MemoryPool,
                @volatile private var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    def header: RequestHeader = context.header
    def sizeOfBodyInBytes: Int = bodyAndSize.size

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def requestDesc(details: Boolean): String = s"$header -- ${loggableRequest.toString(details)}"

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      // 定义一个函数，用于获取可记录的值
      def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
        // 判断资源类型是否敏感
        val maybeSensitive = resourceType match {
          case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
          case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name))
          case ConfigResource.Type.BROKER_LOGGER => false
          case _ => true
        }
        // 如果敏感，则隐藏密码，否则返回原值
        if (maybeSensitive) Password.HIDDEN else value
      }

      // 根据请求类型处理日志记录
      bodyAndSize.request match {
        // 如果是AlterConfigsRequest请求
        case alterConfigs: AlterConfigsRequest =>
          // 转换可记录的配置
          val loggableConfigs = alterConfigs.configs().asScala.map { case (resource, config) =>
            // 创建一个新的AlterConfigsRequest.Config，其中包含可记录的条目
            val loggableEntries = new AlterConfigsRequest.Config(config.entries.asScala.map { entry =>
                new AlterConfigsRequest.ConfigEntry(entry.name, loggableValue(resource.`type`, entry.name, entry.value))
            }.asJavaCollection)
            (resource, loggableEntries)
          }.asJava
          // 使用可记录的配置构建新的AlterConfigsRequest
          new AlterConfigsRequest.Builder(loggableConfigs, alterConfigs.validateOnly).build(alterConfigs.version())

        // 如果是IncrementalAlterConfigsRequest请求
        case alterConfigs: IncrementalAlterConfigsRequest =>
          // 创建一个新的AlterConfigsResourceCollection
          val resources = new AlterConfigsResourceCollection(alterConfigs.data.resources.size)
          // 遍历资源并构建新的AlterConfigsResource
          alterConfigs.data.resources.forEach { resource =>
            val newResource = new AlterConfigsResource()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
            // 遍历配置并添加到新的资源中
            resource.configs.forEach { config =>
              newResource.configs.add(new AlterableConfig()
                .setName(config.name)
                .setValue(loggableValue(ConfigResource.Type.forId(resource.resourceType), config.name, config.value))
                .setConfigOperation(config.configOperation))
            }
            // 将新的资源添加到资源集合中
            resources.add(newResource)
          }
          // 创建新的IncrementalAlterConfigsRequestData并设置资源和验证标志
          val data = new IncrementalAlterConfigsRequestData()
            .setValidateOnly(alterConfigs.data().validateOnly())
            .setResources(resources)
          // 使用新的数据构建新的IncrementalAlterConfigsRequest
          new IncrementalAlterConfigsRequest.Builder(data).build(alterConfigs.version)

        // 其他请求类型，直接返回原始请求
        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * 将纳秒转换为毫秒，并保留微秒精度，因为请求日志中的额外小数位具有较低的信噪比。
       * 在度量标准方面，四舍五入到最接近的长整型后，两者几乎没有区别。
       */
      def nanosToMs(nanos: Long): Double = {
        // 取最大值以确保纳秒数非负
        val positiveNanos = math.max(nanos, 0)
        // 将纳秒转换为微秒，然后转换为毫秒
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      // 计算请求队列时间（毫秒）
      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      // 计算API本地处理时间（毫秒）
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      // 计算API远程处理时间（毫秒）
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      // 计算响应队列时间（毫秒）
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      // 计算响应发送时间（毫秒）
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      // 计算消息转换时间（毫秒）
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      // 计算总时间（毫秒）
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)

      // 根据API键获取度量指标名称
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            // 如果是从follower发起的请求，则使用followFetchMetricName，否则使用consumerFetchMetricName
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty

      // 将fetchMetricNames和apiKey名称添加到metricNames列表中
      val metricNames = fetchMetricNames :+ header.apiKey.name

      // 遍历metricNames列表，更新对应的度量指标
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // 记录网络线程使用时间，这包含在用户/客户端的请求配额中
      // 仅在记录请求处理线程使用情况时执行限流，即在响应排队发送之前
      // 记录的时间是在网络线程上接收请求和发送响应所花费的时间
      // 请注意，对于连接上的第一个请求，时间包括身份验证的总时间，对于SASL/SSL来说，这可能很重要
      // 记录网络线程时间回调
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      // 如果启用了请求日志记录
      if (isRequestLoggingEnabled) {
        val detailsEnabled = requestLogger.underlying.isTraceEnabled
        val responseString = response.responseString.getOrElse(
          throw new IllegalStateException("responseAsString should always be defined if request logging is enabled"))

        // 构建请求日志字符串
        val builder = new StringBuilder(256)
        builder.append("Completed request:").append(requestDesc(detailsEnabled))
          .append(",response:").append(responseString)
          .append(" from connection ").append(context.connectionId)
          .append(";totalTime:").append(totalTimeMs)
          .append(",requestQueueTime:").append(requestQueueTimeMs)
          .append(",localTime:").append(apiLocalTimeMs)
          .append(",remoteTime:").append(apiRemoteTimeMs)
          .append(",throttleTime:").append(apiThrottleTimeMs)
          .append(",responseQueueTime:").append(responseQueueTimeMs)
          .append(",sendTime:").append(responseSendTimeMs)
          .append(",securityProtocol:").append(context.securityProtocol)
          .append(",principal:").append(session.principal)
          .append(",listener:").append(context.listenerName.value)
          .append(",clientInformation:").append(context.clientInformation)

        // 如果使用了临时内存，则添加到日志中
        if (temporaryMemoryBytes > 0)
          builder.append(",temporaryMemoryBytes:").append(temporaryMemoryBytes)

        // 如果存在消息转换时间，则添加到日志中
        if (messageConversionsTimeMs > 0)
          builder.append(",messageConversionsTime:").append(messageConversionsTimeMs)

        // 记录请求日志
        requestLogger.debug(builder.toString)
      }
    }

    def releaseBuffer(): Unit = {
      if (buffer != null) {
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"

  }

  abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseString: Option[String] = Some("")

    def onComplete: Option[Send => Unit] = None

    override def toString: String
  }

  /** responseAsString should only be defined if request logging is enabled */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseAsString: Option[String],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseString: Option[String] = responseAsString

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseAsString)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

class RequestChannel(val queueSize: Int,
                     val metricNamePrefix : String,
                     time: Time) extends KafkaMetricsGroup {
  import RequestChannel._
  val metrics = new RequestChannel.Metrics
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response): Unit = {

    if (isTraceEnabled) {
      val requestHeader = response.request.header
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val RequestsPerSec = "RequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
