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

package com.weareadaptive.cluster.services.infra;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import io.aeron.cluster.service.ClientSession;

/**
 * Interface for responding to auction requests, encapsulating the SBE encoding and Aeron interactions
 */
public interface ClusterClientResponder
{
    void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult);
    void onSuccessMessage(ClientSession session, long correlationId);

}
