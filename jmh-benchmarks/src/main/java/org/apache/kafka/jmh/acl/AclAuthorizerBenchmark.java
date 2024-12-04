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

package org.apache.kafka.jmh.acl;

import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclAuthorizer.VersionedAcls;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.Action;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.JavaConverters;
import scala.collection.immutable.TreeMap;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AclAuthorizerBenchmark {
    @Param({"10000", "50000", "200000"})
    private int resourceCount;
    //no. of. rules per resource
    @Param({"10", "50"})
    private int aclCount;

    private final int hostPreCount = 1000;
    private final String resourceNamePrefix = "foo-bar35_resource-";

    private final AclAuthorizer aclAuthorizer = new AclAuthorizer();
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private List<Action> actions = new ArrayList<>();
    private RequestContext context;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        setFieldValue(aclAuthorizer, AclAuthorizer.class.getDeclaredField("aclCache").getName(),
            prepareAclCache());
        // By adding `-95` to the resource name prefix, we cause the `TreeMap.from/to` call to return
        // most map entries. In such cases, we rely on the filtering based on `String.startsWith`
        // to return the matching ACLs. Using a more efficient data structure (e.g. a prefix
        // tree) should improve performance significantly).
        actions = Collections.singletonList(new Action(AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix + 95, PatternType.LITERAL),
            1, true, true));
        context = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getLocalHost(), KafkaPrincipal.ANONYMOUS,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY);
    }

    private void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private TreeMap<ResourcePattern, VersionedAcls> prepareAclCache() {
        // 创建一个空的HashMap来存储资源模式和对应的AclEntry集合
        Map<ResourcePattern, Set<AclEntry>> aclEntries = new HashMap<>();

        // 遍历所有资源ID
        for (int resourceId = 0; resourceId < resourceCount; resourceId++) {
            // 根据资源ID创建资源模式
            ResourcePattern resource = new ResourcePattern(
                (resourceId % 10 == 0) ? ResourceType.GROUP : ResourceType.TOPIC,
                resourceNamePrefix + resourceId,
                (resourceId % 5 == 0) ? PatternType.PREFIXED : PatternType.LITERAL);

            // 如果aclEntries中不存在该资源模式，则将其添加到aclEntries中
            Set<AclEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

            // 遍历所有AclID
            for (int aclId = 0; aclId < aclCount; aclId++) {
                // 创建一个AccessControlEntry对象
                AccessControlEntry ace = new AccessControlEntry(principal.toString() + aclId,
                    "*", AclOperation.READ, AclPermissionType.ALLOW);
                // 将AclEntry添加到entries集合中
                entries.add(new AclEntry(ace));
            }
        }

        // 创建一个资源模式，表示以resourceNamePrefix为前缀的所有TOPIC资源
        ResourcePattern resourcePrefix = new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix,
            PatternType.PREFIXED);
        // 如果aclEntries中不存在该资源模式，则将其添加到aclEntries中
        Set<AclEntry> entriesPrefix = aclEntries.computeIfAbsent(resourcePrefix, k -> new HashSet<>());
        // 遍历所有主机ID
        for (int hostId = 0; hostId < hostPreCount; hostId++) {
            // 创建一个AccessControlEntry对象
            AccessControlEntry ace = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.READ, AclPermissionType.ALLOW);
            // 将AclEntry添加到entriesPrefix集合中
            entriesPrefix.add(new AclEntry(ace));
        }

        // 创建一个资源模式，表示所有TOPIC资源
        ResourcePattern resourceWildcard = new ResourcePattern(ResourceType.TOPIC, ResourcePattern.WILDCARD_RESOURCE,
            PatternType.LITERAL);
        // 如果aclEntries中不存在该资源模式，则将其添加到aclEntries中
        Set<AclEntry> entriesWildcard = aclEntries.computeIfAbsent(resourceWildcard, k -> new HashSet<>());
        // 获取通配符Acl的动态条目数
        // get dynamic entries number for wildcard acl
        for (int hostId = 0; hostId < resourceCount / 10; hostId++) {
            // 创建一个AccessControlEntry对象
            AccessControlEntry ace = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.READ, AclPermissionType.ALLOW);
            // 将AclEntry添加到entriesWildcard集合中
            entriesWildcard.add(new AclEntry(ace));
        }

        // 创建一个TreeMap来存储资源模式和对应的VersionedAcls
        TreeMap<ResourcePattern, VersionedAcls> aclCache = new TreeMap<>(new AclAuthorizer.ResourceOrdering());
        // 遍历aclEntries中的每个条目
        for (Map.Entry<ResourcePattern, Set<AclEntry>> entry : aclEntries.entrySet()) {
            // 更新aclCache中对应资源模式的VersionedAcls
            aclCache = aclCache.updated(entry.getKey(),
                new VersionedAcls(JavaConverters.asScalaSetConverter(entry.getValue()).asScala().toSet(), 1));
        }

        // 返回aclCache
        return aclCache;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        aclAuthorizer.close();
    }

    @Benchmark
    public void testAclsIterator() {
        aclAuthorizer.acls(AclBindingFilter.ANY);
    }

    @Benchmark
    public void testAuthorizer() {
        aclAuthorizer.authorize(context, actions);
    }
}
