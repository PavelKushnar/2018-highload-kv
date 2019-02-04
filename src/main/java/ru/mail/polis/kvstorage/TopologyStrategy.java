package ru.mail.polis.kvstorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import one.nio.http.Response;

abstract public class TopologyStrategy{

    public static class GetTopologyStrategy extends TopologyStrategy{
        private Map<Long, byte[]> values;

        public GetTopologyStrategy(int ack, int from){
            super(ack, from);
            values = new HashMap<>();
        }

        public void addValue(long timestamp, byte[] value){
            values.put(timestamp, value);
        }

        @Override
        public Response getResponse() {
            int totalAck = getSuccess() + getDeleted() + getNotFound();

            if (totalAck >= getAck() && getSuccess() > 0 && getDeleted() == 0){
                List<Long> times = values.keySet().stream().sorted().collect(Collectors.toList());
                byte[] youngValue = values.get(times.get(times.size() - 1));
                return Response.ok(youngValue);
            } else if (totalAck < getAck()){
                return MyService.createNotEnoughReplicaResponse();
            } else {
                return MyService.createNotFoundResponse();
            }
        }
    }

    public static class PutTopologyStrategy extends TopologyStrategy{
        public PutTopologyStrategy(int ack, int from){
            super(ack, from);
        }

        @Override
        public Response getResponse() {
            return this.getSuccess() >= this.getAck() ? MyService.createSuccessPutResponse() : MyService.createNotEnoughReplicaResponse();
        }
    }

    public static class DeleteTopologyStrategy extends TopologyStrategy{
        public DeleteTopologyStrategy(int ack, int from){
            super(ack, from);
        }

        @Override
        public Response getResponse() {
            return this.getSuccess() >= this.getAck() ? MyService.createSuccessDeleteResponse() : MyService.createNotEnoughReplicaResponse();
        }
    }

    private int success;
    private int deleted;
    private int notFound;
    private int ack;
    private int from;

    public TopologyStrategy(int ack, int from){
        this.ack = ack;
        this.from = from;
    }

    public void addSuccess(){
        success++;
    }

    public void addDeleted(){
        deleted++;
    }

    public void addNotFound(){
        notFound++;
    }

    public int getSuccess() {
        return success;
    }

    public int getDeleted() {
        return deleted;
    }

    public int getNotFound() {
        return notFound;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    abstract public Response getResponse();
}
