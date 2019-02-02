package ru.mail.polis.kvstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

public class MyService extends HttpServer implements KVService{
    public final static String STATUS_ENDPOINT = "/v0/status";
    public final static String STORAGE_ENDPOINT = "/v0/entity";
    public final static String ID_PARAMETER = "id=";
    public final static String REPLICAS_PARAMETER = "replicas=";
    public final static String QUERY_FROM_REPLICA = "from-replica";
    public final static String UPDATED_VALUE_HEADER = "updated: ";

    public final static Response NOT_ENOUGH_REPLICAS_RESPONSE = new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    public final static Response SUCCESS_PUT_RESPONSE = new Response(Response.CREATED, Response.EMPTY);
    public final static Response SUCCESS_DELETE_RESPONSE = new Response(Response.ACCEPTED, Response.EMPTY);

    private final Set<String> topology;
    private final List<String> topologyList;
    private final Map<String, HttpClient> clients;
    private final MyDAO dao;
    private final String myHost;

    public MyService(HttpServerConfig config, KVDao dao, Set<String> topology) throws IOException{
        super(config);
        this.topology = topology;
        this.topologyList = new ArrayList<>(topology);
        this.dao = (MyDAO)dao;

        myHost = "http://localhost:" + config.acceptors[0].port;

        clients = new HashMap<>();
        for (String host : topology){
            if (!host.equals(myHost)){
                clients.put(host, new HttpClient(new ConnectionString(host)));
            }
        }
    }

    @Path(MyService.STATUS_ENDPOINT)
    public Response statusEndpoint(Request request){
        return Response.ok(Response.EMPTY);
    }

    @Path(MyService.STORAGE_ENDPOINT)
    public Response storageEndpoint(Request request){
        try {
            switch (request.getMethod()){
                case Request.METHOD_GET:
                    return handleGet(request);
                case Request.METHOD_PUT:
                    return handlePut(request);
                case Request.METHOD_DELETE:
                    return handleDelete(request);
            }
        } catch (IllegalArgumentException e){
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        return new Response(Response.BAD_GATEWAY, Response.EMPTY);
    }

    private Response handleGet(Request request) throws IllegalArgumentException{
        byte[] key = getKey(request);
        if (fromReplica(request)){
            Response response;
            try{
                response = new Response(Response.OK, dao.get(key));
            } catch (NoSuchElementException e) {
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
            } catch (IOException e) {
                e.printStackTrace();
                response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
            long time = dao.getTime(key);
            if (time > 0) {
                response.addHeader(UPDATED_VALUE_HEADER + time);
            }
            return response;
        } else{
            AckFrom ackFrom = new AckFrom(request);
            TopologyStrategy.GetTopologyStrategy strategy = new TopologyStrategy.GetTopologyStrategy(ackFrom.ack, ackFrom.from);
            for (String host : getHostsForKey(key, ackFrom.from)){
                if (host.equals(myHost)) {
                    try {
                        strategy.addValue(dao.getTime(key), dao.get(key));
                        strategy.addSuccess();
                    } catch (NoSuchElementException e) {
                        long time = dao.getTime(key);
                        if (time > 0){
                            strategy.addDeleted();
                        } else {
                            strategy.addNotFound();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else{
                    try {
                        Response response = clients.get(host).get(request.getURI(), QUERY_FROM_REPLICA);
                        String timeStr = response.getHeader(UPDATED_VALUE_HEADER);
                        Long time = null;
                        if (timeStr != null) {
                            time = Long.parseLong(timeStr);
                        }
                        if (response.getStatus() == 200){
                            strategy.addSuccess();
                            strategy.addValue(Integer.parseInt(response.getHeader(UPDATED_VALUE_HEADER)), response.getBody());
                        } else if (response.getStatus() == 404){
                            if (time != null) {
                                strategy.addDeleted();
                            } else {
                                strategy.addNotFound();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return strategy.getResponse();
        }

    }

    private Response handlePut(Request request) throws IllegalArgumentException{
        byte[] key = getKey(request);
        byte[] value = request.getBody();
        if (fromReplica(request)){
            Response response;
            try {
                dao.upsert(key, value);
                response = SUCCESS_PUT_RESPONSE;
            } catch (IOException e){
                e.printStackTrace();
                response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
            return response;
        } else{
            AckFrom ackFrom = new AckFrom(request);
            TopologyStrategy.PutTopologyStrategy strategy = new TopologyStrategy.PutTopologyStrategy(ackFrom.ack, ackFrom.from);
            for (String host : getHostsForKey(key, ackFrom.from)){
                if (host.equals(myHost)){
                    try {
                        dao.upsert(key, value);
                        strategy.addSuccess();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                } else{
                    try {
                        Response response = clients.get(host).put(request.getURI(), value, QUERY_FROM_REPLICA);
                        if (response.getStatus() == 201){
                            strategy.addSuccess();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return strategy.getResponse();
        }
    }

    private Response handleDelete(Request request) throws IllegalArgumentException{
        byte[] key = getKey(request);
        if (fromReplica(request)){
            Response response;
            try {
                dao.remove(key);
                response = SUCCESS_DELETE_RESPONSE;
            } catch (IOException e){
                e.printStackTrace();
                response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
            return response;
        } else{
            AckFrom ackFrom = new AckFrom(request);
            TopologyStrategy.DeleteTopologyStrategy strategy = new TopologyStrategy.DeleteTopologyStrategy(ackFrom.ack, ackFrom.from);
            for (String host : getHostsForKey(key, ackFrom.from)){
                if (host.equals(myHost)){
                    try {
                        dao.remove(key);
                        strategy.addSuccess();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else{
                    try {
                        Response response = clients.get(host).delete(request.getURI(), QUERY_FROM_REPLICA);
                        if (response.getStatus() == 202){
                            strategy.addSuccess();
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
            return strategy.getResponse();
        }
    }

    private byte[] getKey(Request request) throws IllegalArgumentException{
        String id = request.getParameter(ID_PARAMETER);
        if (id == null || id.isEmpty()){
            throw new IllegalArgumentException();
        }
        return id.getBytes();
    }

    private List<String> getHostsForKey(byte[] key, int from){
        int hash = 0;
        for (byte k : key){
            hash += k;
        }
        int index = Math.abs(hash);
        int hostsCount = topologyList.size();
        List<String> hosts = new ArrayList<>(from);
        for (int i = index + from; i > index; i--){
            hosts.add(topologyList.get(i % hostsCount));
        }
        return hosts;
    }

    private boolean fromReplica(Request request){
        return request.getHeader(QUERY_FROM_REPLICA) != null;
    }

    private class AckFrom{
        final int ack;
        final int from;

        public AckFrom(Request request) throws IllegalArgumentException{
            String replicas = request.getParameter(REPLICAS_PARAMETER);
            if (replicas == null){
                int size = topology.size();
                this.ack = size / 2 + 1;
                this.from = size;
            } else {
                try {
                    String[] split = replicas.split("/");
                    int ack = Integer.parseInt(split[0]);
                    int from = Integer.parseInt(split[1]);
                    if (ack < 1 || ack > from){
                        throw new Exception();
                    }
                    this.ack = ack;
                    this.from = from;
                } catch (Exception e){
                    throw new IllegalArgumentException();
                }
            }
        }
    }
}
