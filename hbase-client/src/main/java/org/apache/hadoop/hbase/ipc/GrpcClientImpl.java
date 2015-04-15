package org.apache.hadoop.hbase.ipc;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ClientCall;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of RpcClient using GRPC.
 *
 * Notes.... - Should refactor static utility functions out of AbstractRpcClient ? - Not using the
 * compressor yet - Need to handle fallback. See #fallbackAllowed in AbstractRpcClient - Removed use
 * of Hadoop SocketFactory (review?)
 */
public class GrpcClientImpl extends AbstractRpcClient {

  private static final Log LOG = LogFactory.getLog(GrpcClientImpl.class);

  private final AtomicBoolean running = new AtomicBoolean(true); // if client runs

  private final PoolMap<ConnectionId, ManagedChannel> channels;

  private final GrpcUtil grpcUtil;

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   *
   * @param conf      configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address
   */
  GrpcClientImpl(Configuration conf, String clusterId, SocketAddress localAddr) {
    super(conf, clusterId, localAddr);
    // TODO(lryan): GRPC should provide a keyed connection pool as a utility. Should fix the
    // keying here to be user + address only as service is multiplexed. If auth moves into
    // headers then the key is simply address
    channels = new PoolMap<ConnectionId, ManagedChannel>(getPoolType(conf), 1);
    grpcUtil = new GrpcUtil(this.ipcUtil, getCodec());
  }

  /**
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   *
   * @param conf      configuration
   * @param clusterId the cluster id
   */
  public GrpcClientImpl(Configuration conf, String clusterId) {
    this(conf, clusterId, null);
  }

  /**
   * Get a connection from the pool, or create a new one and add it to the pool. Connections to a
   * given host/port are reused.
   */
  Channel getChannel(User ticket, String serviceName, InetSocketAddress addr)
      throws IOException {
    if (!running.get()) throw new StoppedRpcClientException();
    ManagedChannel channel = null;
    ConnectionId remoteId = new ConnectionId(ticket, serviceName, addr);
    synchronized (channels) {
      if (channels.size(remoteId) != 0) {
        channel = channels.get(remoteId);
      }
      if (channel == null) {
        channel = createChannel(remoteId);
        channels.put(remoteId, channel);
      }
    }
    return channel;
  }

  /**
   * Creates a connection. Can be overridden by a subclass for testing.
   *
   * @param remoteId - the ConnectionId to use for the connection creation.
   */
  ManagedChannel createChannel(ConnectionId remoteId) {
    return ManagedChannelBuilder.forAddress(remoteId.getAddress().getHostName(),
        remoteId.getAddress().getPort())
        .usePlaintext(true)
        .executor(MoreExecutors.newDirectExecutorService())
        .build();
  }

  @Override
  protected Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc,
        MethodDescriptor md, Message param, Message returnType,
        User ticket, InetSocketAddress isa) throws IOException, InterruptedException {
    if (pcrc == null) {
      pcrc = new PayloadCarryingRpcController();
    }
    CellScanner cells = pcrc.cellScanner();
    final Channel channel = getChannel(ticket, md.getService().getName(), isa);

    ClientCall<Pair<Message, CellScanner>, Pair<Message, CellScanner>> call =
        channel.newCall(grpcUtil.getMethodDesriptor(md, pcrc.getCallTimeout()),
            CallOptions.DEFAULT);

    // TODO(lryan): Doing blocking here but could easily do async too.
    return ClientCalls.blockingUnaryCall(call, new Pair(param, cells));
  }

  protected void callAsync(PayloadCarryingRpcController pcrc,
                                            MethodDescriptor md, Message param, Message returnType,
                                            User ticket, InetSocketAddress isa,
                                            StreamObserver<Pair<Message, CellScanner>> observer)
      throws IOException, InterruptedException {
    if (pcrc == null) {
      pcrc = new PayloadCarryingRpcController();
    }
    CellScanner cells = pcrc.cellScanner();
    final Channel channel = getChannel(ticket, md.getService().getName(), isa);

    ClientCall<Pair<Message, CellScanner>, Pair<Message, CellScanner>> call =
        channel.newCall(grpcUtil.getMethodDesriptor(md, pcrc.getCallTimeout()),
            CallOptions.DEFAULT);

    // TODO(lryan): Doing blocking here but could easily do async too.
    ClientCalls.asyncUnaryCall(call, new Pair(param, cells),  observer);
  }

  @Override
  public void cancelConnections(ServerName sn) {

  }

  @Override
  public void close() {
    if (LOG.isDebugEnabled()) LOG.debug("Stopping grpc client");
    if (!running.compareAndSet(true, false)) return;

    synchronized (channels) {
      // shutdown all the channels
      for (ManagedChannel channel : channels.values()) {
        channel.shutdown();
      }

      // wait until all channels are closed
      while (!channels.isEmpty()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Interrupted while stopping the client. We still have " + channels.size() +
              " connections.");
          Thread.currentThread().interrupt();
          return;
        }
        for (ManagedChannel channel : new ArrayList<>(channels.values())) {
          if (channel.isShutdown()) {
            channels.remove(channel);
          }
        }
      }
    }
  }
}