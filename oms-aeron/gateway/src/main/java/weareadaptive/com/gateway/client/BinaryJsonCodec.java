package weareadaptive.com.gateway.client;

import com.weareadaptive.sbe.ExecutionResultDecoder;
import com.weareadaptive.sbe.OrderDecoder;
import com.weareadaptive.sbe.OrderIdDecoder;
import com.weareadaptive.sbe.SuccessMessageDecoder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.agrona.DirectBuffer;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BinaryJsonCodec
{
    private final Map<Long, List<Order>> ordersRequests = new ConcurrentHashMap<>();
    final ExecutionResult executionResult = new ExecutionResult();
    private final OrderIdDecoder orderIdDecoder = new OrderIdDecoder();
    private final ExecutionResultDecoder executionResultDecoder = new ExecutionResultDecoder();
    private final SuccessMessageDecoder successMessageDecoder = new SuccessMessageDecoder();
    private final OrderDecoder orderDecoder = new OrderDecoder();
    protected JsonObject getOrderIdResponse(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        orderIdDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return JsonObject.of("orderId", orderIdDecoder.orderId());
    }

    protected JsonObject getExecutionResultAsJson(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        executionResultDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final long orderId = executionResultDecoder.orderId();
        final Status status = Status.fromByteValue((byte) executionResultDecoder.status());
        executionResult.setStatus(status);
        executionResult.setOrderId(orderId);
        return JsonObject.mapFrom(executionResult);
    }

    protected JsonObject getSuccessMessageAsJson(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        successMessageDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final Status status = Status.fromByteValue((byte) successMessageDecoder.status());
        return JsonObject.of("status", status.name());
    }

    protected Order getOrder(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion) {
        orderDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return new Order(orderDecoder.orderId(), orderDecoder.price(), orderDecoder.size());
    }

    public void addOrderToCollectionOfAllOrders(final long correlationId, final DirectBuffer buffer, final int offset,
                                                final int actingBlockLength, final int actingVersion) {
        Order order = getOrder(buffer, offset, actingBlockLength, actingVersion);
        List<Order> listToUpdate = ordersRequests.getOrDefault(correlationId, new ArrayList<>());
        listToUpdate.add(order);
        ordersRequests.put(correlationId, listToUpdate);
    }

    protected JsonObject getOrdersResponse(final long correlationId)
    {
        List<Order> orders = ordersRequests.get(correlationId);
        if (orders == null) {
            orders = new ArrayList<>();
        }
        JsonArray jsonArray = new JsonArray();
        for (Order order : orders)
        {
            JsonObject orderJson = JsonObject.mapFrom(order);
            jsonArray.add(orderJson);
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("orders", jsonArray);
        return jsonObject;
    }
}
