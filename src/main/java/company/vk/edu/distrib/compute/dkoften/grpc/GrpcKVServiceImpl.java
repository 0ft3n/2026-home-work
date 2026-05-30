package company.vk.edu.distrib.compute.dkoften.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.dkoften.storage.DaoImpl;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

public class GrpcKVServiceImpl extends KVServiceGrpcGrpc.KVServiceGrpcImplBase {
    private final DaoImpl dao;
    private final Logger logger = LoggerFactory.getLogger("grpc");

    public GrpcKVServiceImpl(DaoImpl dao) {
        super();
        this.dao = dao;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("gRPC GET request for key: {}", request.getKey());
            }
            byte[] value = dao.get(request.getKey());
            GetResponse response = GetResponse.newBuilder()
                    .setStatus(200)
                    .setValue(ByteString.copyFrom(value))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            GetResponse response = GetResponse.newBuilder()
                    .setStatus(404)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error processing gRPC GET", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("gRPC PUT request for key: {}", request.getKey());
            }
            dao.upsert(request.getKey(), request.getValue().toByteArray());
            PutResponse response = PutResponse.newBuilder()
                    .setStatus(201)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error processing gRPC PUT", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("gRPC DELETE request for key: {}", request.getKey());
            }
            dao.delete(request.getKey());
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setStatus(202)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error processing gRPC DELETE", e);
            responseObserver.onError(e);
        }
    }
}
