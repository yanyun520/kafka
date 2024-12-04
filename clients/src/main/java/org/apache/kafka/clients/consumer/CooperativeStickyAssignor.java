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
package org.apache.kafka.clients.consumer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

/**
 * A cooperative version of the {@link AbstractStickyAssignor AbstractStickyAssignor}. This follows the same (sticky)
 * assignment logic as {@link StickyAssignor StickyAssignor} but allows for cooperative rebalancing while the
 * {@link StickyAssignor StickyAssignor} follows the eager rebalancing protocol. See
 * {@link ConsumerPartitionAssignor.RebalanceProtocol} for an explanation of the rebalancing protocols.
 * <p>
 * Users should prefer this assignor for newer clusters.
 * <p>
 * To turn on cooperative rebalancing you must set all your consumers to use this {@code PartitionAssignor},
 * or implement a custom one that returns {@code RebalanceProtocol.COOPERATIVE} in
 * {@link CooperativeStickyAssignor#supportedProtocols supportedProtocols()}.
 * <p>
 * IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order to safely turn on
 * cooperative rebalancing. See the <a href="https://kafka.apache.org/documentation/#upgrade_240_notable">upgrade guide</a> for details.
 */
public class CooperativeStickyAssignor extends AbstractStickyAssignor {
    public static final String COOPERATIVE_STICKY_ASSIGNOR_NAME = "cooperative-sticky";

    // these schemas are used for preserving useful metadata for the assignment, such as the last stable generation
    private static final String GENERATION_KEY_NAME = "generation";
    private static final Schema COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0 = new Schema(
        new Field(GENERATION_KEY_NAME, Type.INT32));

    private int generation = DEFAULT_GENERATION; // consumer group generation



    @Override
    public String name() {
        return COOPERATIVE_STICKY_ASSIGNOR_NAME;
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return Arrays.asList(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        this.generation = metadata.generationId();
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        Struct struct = new Struct(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0);

        struct.set(GENERATION_KEY_NAME, generation);
        ByteBuffer buffer = ByteBuffer.allocate(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    @Override
    protected MemberData memberData(Subscription subscription) {
        ByteBuffer buffer = subscription.userData();
        Optional<Integer> encodedGeneration;
        if (buffer == null) {
            encodedGeneration = Optional.empty();
        } else {
            try {
                Struct struct = COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.read(buffer);
                encodedGeneration = Optional.of(struct.getInt(GENERATION_KEY_NAME));
            } catch (Exception e) {
                encodedGeneration = Optional.of(DEFAULT_GENERATION);
            }
        }
        return new MemberData(subscription.ownedPartitions(), encodedGeneration);
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignments = super.assign(partitionsPerTopic, subscriptions);

        Map<TopicPartition, String> partitionsTransferringOwnership = super.partitionsTransferringOwnership == null ?
            computePartitionsTransferringOwnership(subscriptions, assignments) :
            super.partitionsTransferringOwnership;

        adjustAssignment(assignments, partitionsTransferringOwnership);
        return assignments;
    }

    // Following the cooperative rebalancing protocol requires removing partitions that must first be revoked from the assignment
    private void adjustAssignment(Map<String, List<TopicPartition>> assignments,
                                  Map<TopicPartition, String> partitionsTransferringOwnership) {
        for (Map.Entry<TopicPartition, String> partitionEntry : partitionsTransferringOwnership.entrySet()) {
            assignments.get(partitionEntry.getValue()).remove(partitionEntry.getKey());
        }
    }

    private Map<TopicPartition, String> computePartitionsTransferringOwnership(Map<String, Subscription> subscriptions,
                                                                               Map<String, List<TopicPartition>> assignments) {
        // 创建一个存储所有新增分区的映射表
        Map<TopicPartition, String> allAddedPartitions = new HashMap<>();
        // 创建一个存储所有被撤销分区的集合
        Set<TopicPartition> allRevokedPartitions = new HashSet<>();

        // 遍历每个消费者的分区分配情况
        for (final Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            // 获取当前消费者的ID
            String consumer = entry.getKey();

            // 获取当前消费者拥有的分区列表
            List<TopicPartition> ownedPartitions = subscriptions.get(consumer).ownedPartitions();
            // 获取当前消费者被分配的分区列表
            List<TopicPartition> assignedPartitions = entry.getValue();

            // 将当前消费者拥有的分区列表转换为集合，方便快速查找
            Set<TopicPartition> ownedPartitionsSet = new HashSet<>(ownedPartitions);
            // 遍历当前消费者被分配的分区列表
            for (TopicPartition tp : assignedPartitions) {
                // 如果该分区不在当前消费者拥有的分区集合中，则将其添加到新增分区映射表中
                if (!ownedPartitionsSet.contains(tp))
                    allAddedPartitions.put(tp, consumer);
            }

            // 将当前消费者被分配的分区列表转换为集合，方便快速查找
            Set<TopicPartition> assignedPartitionsSet = new HashSet<>(assignedPartitions);
            // 遍历当前消费者拥有的分区列表
            for (TopicPartition tp : ownedPartitions) {
                // 如果该分区不在当前消费者被分配的分区集合中，则将其添加到被撤销分区集合中
                if (!assignedPartitionsSet.contains(tp))
                    allRevokedPartitions.add(tp);
            }
        }

        // 只保留在新增分区映射表中且在被撤销分区集合中的分区
        allAddedPartitions.keySet().retainAll(allRevokedPartitions);
        // 返回新增分区映射表
        return allAddedPartitions;
    }
}
