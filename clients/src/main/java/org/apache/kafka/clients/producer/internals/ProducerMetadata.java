/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    // 主题元数据过期时间，如果在这个时间段内未被访问，它就会从缓存中删除。
    private final long metadataIdleMs;

    /* Topics with expiry time */
    // map集合，生产者的元数据主题集合，里面保存着
    // 主题和主题过期时间的对应关系，即 topic, nowMs + metadataIdleMs，
    // 过期了的主题会被踢出去。
    private final Map<String, Long> topics = new HashMap<>();
    // 新的主题集合, set集合，即第一次要发送的主题
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            requestUpdateForNewTopics();
        }
    }

    /**
     * 该方法用来判断是全量更新元数据还是部分更新元数据，逻辑相对比较简单，主要用在 KafkaProducer 主线程元数据同步等待时调用
     * @param topic
     * @return
     */
    public synchronized int requestUpdateForTopic(String topic) {
        // 如果新主题集合中存在该主题
        if (newTopics.contains(topic)) {
            // 针对新主题集合标记部分更新，并返回版本
            return requestUpdateForNewTopics();
        } else {
            // 全量更新，并返回版本
            return requestUpdate();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * 该方法用来判断元数据中是否该保留该主题，会在 handleMetadataResponse 即处理元数据响应结果的时候进行调用，我们来看下它是如何判断的。
     * <p>
         先判断元数据主题集合中是否存在该主题，如果不存在直接返回false。
         然后判断该主题是否在新主题集合中，如果存在直接返回true。
         再判断该主题是否超过了过期时间，如果超过了，就从元数据主题集合中删除该主题，再请求元数据的时候就不用带上该主题，可以有效的减少网络传输数据大小。
     * </p>
     *
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        // 获取该主题的过期时间
        Long expireMs = topics.get(topic);
        // 如果为空表示该主题不在元数据主题集合中
        if (expireMs == null) {
            return false;
            // 判断该主题是否在新集合中
        } else if (newTopics.contains(topic)) {
            return true;
            // 判断是否超过了过期时间
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            // 超过后直接从元数据主题集合中删除该主题
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        super.update(requestVersion, response, isPartialUpdate, nowMs);

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

        notifyAll();
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
