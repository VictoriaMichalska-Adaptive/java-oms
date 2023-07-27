package com.weareadaptive.cluster.services.infra;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import io.aeron.cluster.service.ClientSession;

import static com.weareadaptive.util.Codec.*;
import static com.weareadaptive.util.Codec.SUCCESS_MESSAGE_SIZE;

public class ClusterClientResponderImpl implements ClusterClientResponder
{
    @Override
    public void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult)
    {
        while (session.offer(encodeExecutionResult(correlationId, executionResult), 0, EXECUTION_RESULT_SIZE) < 0);
    }

    @Override
    public void onSuccessMessage(ClientSession session, long correlationId)
    {
        while (session.offer(encodeSuccessMessage(correlationId), 0, SUCCESS_MESSAGE_SIZE) < 0);
    }
}
