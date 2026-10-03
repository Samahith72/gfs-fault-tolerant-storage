package io.gfs.master;

import io.gfs.proto.CheckPrimaryRequest;
import io.gfs.proto.CheckPrimaryResponse;
import io.gfs.proto.GetClusterStatusRequest;
import io.gfs.proto.GetClusterStatusResponse;
import io.gfs.proto.GetPrimaryRequest;
import io.gfs.proto.GetPrimaryResponse;
import io.gfs.proto.HeartbeatRequest;
import io.gfs.proto.HeartbeatResponse;
import io.gfs.proto.LocateChunkRequest;
import io.gfs.proto.LocateChunkResponse;
import io.gfs.proto.MasterServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class MasterGrpcService extends MasterServiceGrpc.MasterServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(MasterGrpcService.class);

    private final ClusterState state;

    public MasterGrpcService(ClusterState state) {
        this.state = state;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> out) {
        try {
            out.onNext(state.heartbeat(request.getServerId(), request.getChunksList()));
            out.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn("Rejected heartbeat: {}", e.getMessage());
            out.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getPrimary(GetPrimaryRequest request, StreamObserver<GetPrimaryResponse> out) {
        out.onNext(state.getPrimary());
        out.onCompleted();
    }

    @Override
    public void checkPrimary(CheckPrimaryRequest request, StreamObserver<CheckPrimaryResponse> out) {
        try {
            out.onNext(state.checkPrimary(request.getServerId()));
            out.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn("Rejected CheckPrimary: {}", e.getMessage());
            out.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void locateChunk(LocateChunkRequest request, StreamObserver<LocateChunkResponse> out) {
        out.onNext(LocateChunkResponse.newBuilder()
                .addAllServers(state.locate(request.getChunkHandle()))
                .build());
        out.onCompleted();
    }

    @Override
    public void getClusterStatus(GetClusterStatusRequest request, StreamObserver<GetClusterStatusResponse> out) {
        out.onNext(state.clusterStatus());
        out.onCompleted();
    }
}