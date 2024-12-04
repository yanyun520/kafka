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
package org.apache.kafka.clients.admin;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.KafkaFuture.BaseFunction;
import org.apache.kafka.common.KafkaFuture.BiConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

/**
 * The result of the {@link AdminClient#alterConsumerGroupOffsets(String, Map)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class AlterConsumerGroupOffsetsResult {

    private final KafkaFuture<Map<TopicPartition, Errors>> future;

    AlterConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future) {
        this.future = future;
    }

    /**
     * Return a future which can be used to check the result for a given partition.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        // 创建一个KafkaFutureImpl实例来存储结果
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        // 当this.future完成时，执行以下逻辑
        this.future.whenComplete(new BiConsumer<Map<TopicPartition, Errors>, Throwable>() {
            @Override
            public void accept(final Map<TopicPartition, Errors> topicPartitions, final Throwable throwable) {
                // 如果throwable不为空，表示发生异常，将异常传递给result
                if (throwable != null) {
                    result.completeExceptionally(throwable);
                }
                // 如果throwable为空，但topicPartitions不包含指定的partition，表示没有尝试更改该partition的偏移量
                else if (!topicPartitions.containsKey(partition)) {
                    // 抛出非法参数异常
                    result.completeExceptionally(new IllegalArgumentException(
                        "Alter offset for partition \"" + partition + "\" was not attempted"));
                } else {
                    // 获取指定partition的错误信息
                    final Errors error = topicPartitions.get(partition);
                    // 如果没有错误，表示操作成功，完成result
                    if (error == Errors.NONE) {
                        result.complete(null);
                    }
                    // 如果有错误，将错误信息传递给result
                    else {
                        result.completeExceptionally(error.exception());
                    }
                }

            }
        });

        // 返回result
        return result;
    }

    /**
     * Return a future which succeeds if all the alter offsets succeed.
     */
    public KafkaFuture<Void> all() {
        return this.future.thenApply(new BaseFunction<Map<TopicPartition, Errors>, Void>() {
            @Override
            public Void apply(final Map<TopicPartition, Errors> topicPartitionErrorsMap) {
                List<TopicPartition> partitionsFailed = topicPartitionErrorsMap.entrySet()
                        .stream()
                        .filter(e -> e.getValue() != Errors.NONE)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                for (Errors error : topicPartitionErrorsMap.values()) {
                    if (error != Errors.NONE) {
                        throw error.exception(
                            "Failed altering consumer group offsets for the following partitions: " + partitionsFailed);
                    }
                }
                return null;
            }
        });
    }
}
