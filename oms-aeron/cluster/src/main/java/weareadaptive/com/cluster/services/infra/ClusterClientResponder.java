/*
 * Copyright 2023 Adaptive Financial Consulting
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package weareadaptive.com.cluster.services.infra;

import io.aeron.cluster.service.ClientSession;
import org.agrona.concurrent.IdleStrategy;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;

import java.util.TreeSet;

/**
 * Interface for responding to orderbook requests, encapsulating the SBE encoding and Aeron interactions
 */
public interface ClusterClientResponder
{
    void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult);
    void onSuccessMessage(ClientSession session, long correlationId);
    void onOrders(ClientSession session, long messageId, TreeSet<Order> orders);
    void onOrderId(ClientSession session, long messageId, long currentOrderId);
    void setIdleStrategy(IdleStrategy idleStrategy);
}
