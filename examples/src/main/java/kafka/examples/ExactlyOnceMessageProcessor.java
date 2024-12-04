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
package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A demo class for how to write a customized EOS app. It takes a consume-process-produce loop.
 * Important configurations and APIs are commented.
 */
public class ExactlyOnceMessageProcessor extends Thread {

    private static final boolean READ_COMMITTED = true;

    private final String inputTopic;
    private final String outputTopic;
    private final String transactionalId;
    private final String groupInstanceId;

    private final KafkaProducer<Integer, String> producer;
    private final KafkaConsumer<Integer, String> consumer;

    private final CountDownLatch latch;

    public ExactlyOnceMessageProcessor(final String inputTopic,
                                       final String outputTopic,
                                       final int instanceIdx,
                                       final CountDownLatch latch) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.transactionalId = "Processor-" + instanceIdx;
        // It is recommended to have a relatively short txn timeout in order to clear pending offsets faster.
        final int transactionTimeoutMs = 10000;
        // A unique transactional.id must be provided in order to properly use EOS.
        producer = new Producer(outputTopic, true, transactionalId, true, -1, transactionTimeoutMs, null).get();
        // Consumer must be in read_committed mode, which means it won't be able to read uncommitted data.
        // Consumer could optionally configure groupInstanceId to avoid unnecessary rebalances.
        this.groupInstanceId = "Txn-consumer-" + instanceIdx;
        consumer = new Consumer(inputTopic, "Eos-consumer",
            Optional.of(groupInstanceId), READ_COMMITTED, -1, null).get();
        this.latch = latch;
    }

    @Override
    public void run() {
        // 初始化事务调用应该始终首先发生，以便清除上一代中的僵尸事务。
        // Init transactions call should always happen first in order to clear zombie transactions from previous generation.
        producer.initTransactions();

        final AtomicLong messageRemaining = new AtomicLong(Long.MAX_VALUE);

        consumer.subscribe(Collections.singleton(inputTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 撤销分区分配以启动重新平衡。
                // Revoked partition assignment to kick-off rebalancing:
                printWithTxnId("Revoked partition assignment to kick-off rebalancing: " + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 在重新平衡后接收分区分配。
                // Received partition assignment after rebalancing:
                printWithTxnId("Received partition assignment after rebalancing: " + partitions);
                messageRemaining.set(messagesRemaining(consumer));
            }
        });

        int messageProcessed = 0;
        while (messageRemaining.get() > 0) {
            try {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.count() > 0) {
                    // 开始一个新的事务会话。
                    // Begin a new transaction session.
                    producer.beginTransaction();
                    for (ConsumerRecord<Integer, String> record : records) {
                        // 处理记录并将其发送到下游。
                        // Process the record and send to downstream.
                        ProducerRecord<Integer, String> customizedRecord = transform(record);
                        producer.send(customizedRecord);
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = consumerOffsets();

                    // 通过将偏移量发送到组协调器代理来检查进度。
                    // Note that this API is only available for broker >= 2.5.
                    // Checkpoint the progress by sending offsets to group coordinator broker.
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                    // 完成事务。现在，所有发送的记录都应可供消费。
                    // Finish the transaction. All sent records should be visible for consumption now.
                    producer.commitTransaction();
                    messageProcessed += records.count();
                }
            } catch (ProducerFencedException e) {
                // 如果事务ID已被另一个进程占用，则抛出Kafka异常。
                throw new KafkaException(String.format("The transactional.id %s has been claimed by another process", transactionalId));
            } catch (FencedInstanceIdException e) {
                // 如果组实例ID已被另一个进程占用，则抛出Kafka异常。
                throw new KafkaException(String.format("The group.instance.id %s has been claimed by another process", groupInstanceId));
            } catch (KafkaException e) {
                // 如果我们没有被围起来，尝试中止事务并继续。如果生产者遇到致命错误，这将立即引发。
                // If we have not been fenced, try to abort the transaction and continue. This will raise immediately
                // if the producer has hit a fatal error.
                producer.abortTransaction();

                // 消费者的获取位置需要恢复到事务开始之前的提交偏移量。
                // The consumer fetch position needs to be restored to the committed offset
                // before the transaction started.
                resetToLastCommittedPositions(consumer);
            }

            // 更新剩余消息数量。
            messageRemaining.set(messagesRemaining(consumer));
            // 打印剩余消息数量。
            printWithTxnId("Message remaining: " + messageRemaining);
        }

        // 打印已处理的消息数量。
        printWithTxnId("Finished processing " + messageProcessed + " records");
        latch.countDown();
    }

    private Map<TopicPartition, OffsetAndMetadata> consumerOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            offsets.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return offsets;
    }

    private void printWithTxnId(final String message) {
        System.out.println(transactionalId + ": " + message);
    }

    private ProducerRecord<Integer, String> transform(final ConsumerRecord<Integer, String> record) {
        printWithTxnId("Transformed record (" + record.key() + "," + record.value() + ")");
        return new ProducerRecord<>(outputTopic, record.key() / 2, "Transformed_" + record.value());
    }

    private long messagesRemaining(final KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, Long> fullEndOffsets = consumer.endOffsets(new ArrayList<>(consumer.assignment()));
        // If we couldn't detect any end offset, that means we are still not able to fetch offsets.
        if (fullEndOffsets.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            printWithTxnId("Processing partition " + partition + " with full offsets " + fullEndOffsets);
            if (fullEndOffsets.containsKey(partition)) {
                return fullEndOffsets.get(partition) - currentPosition;
            }
            return 0;
        }).sum();
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null)
                consumer.seek(tp, offsetAndMetadata.offset());
            else
                consumer.seekToBeginning(Collections.singleton(tp));
        });
    }
}
