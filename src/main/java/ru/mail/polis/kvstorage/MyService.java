package ru.mail.polis.kvstorage;

import java.io.IOException;
import java.util.Set;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

public class MyService extends HttpServer implements KVService{
    public final static String STATUS_ENDPOINT = "/v0/status";
    public final static String STORAGE_ENDPOINT = "/v0/entity";
    public final static String ID_PARAMETER = "id=";
    public final static String REPLICAS_PARAMETER = "replicas=";
    private final Set<String> topology;
    private final KVDao dao;

    public MyService(HttpServerConfig config, KVDao dao, Set<String> topology) throws IOException{
        super(config);
        this.topology = topology;
        this.dao = dao;
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
        int ack = getAck(request);
        int from = getFrom(request);

        return dao.get(key);
    }

    private Response handlePut(Request request) throws IllegalArgumentException{
        byte[] key = getKey(request);
        int ack = getAck(request);
        int from = getFrom(request);
    }

    private Response handleDelete(Request request) throws IllegalArgumentException{
        byte[] key = getKey(request);
        int ack = getAck(request);
        int from = getFrom(request);
    }

    private byte[] getKey(Request request) throws IllegalArgumentException{
        String id = request.getParameter(ID_PARAMETER);
        if (id == null || id.isEmpty()){
            throw new IllegalArgumentException();
        }
        return id.getBytes();
    }

    private int getAck(Request request) throws IllegalArgumentException{
        String replicas = request.getParameter(REPLICAS_PARAMETER);
        if (replicas == null) {
            return topology.size() / 2 + 1;
        } else {
            try {
                return Integer.parseInt(replicas.split("/")[0]);
            } catch (Exception e){
                throw new IllegalArgumentException();
            }
        }
    }

    private int getFrom(Request request) throws IllegalArgumentException{
        String replicas = request.getParameter(REPLICAS_PARAMETER);
        if (replicas == null) {
            return topology.size();
        } else {
            try {
                return Integer.parseInt(replicas.split("/")[1]);
            } catch (Exception e){
                throw new IllegalArgumentException();
            }
        }
    }
}
