/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandlerImpl;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;

/**
 * A knock-off of RpcServer but which uses GRPC as the transport mechanism. This implementation
 * adapts the old-style generated proto service interfaces so that the same ones can be bound
 * to GRPC as are bound to the built-in HBase RpcServer. It does not use the shinier generated
 * GRPC stubs as that would entail a larger set of interface updates throughout the codebase.
 *
 * Implementation notes & todos....
 * - Need to decide what to do about RPC scheduling and priorities in GRPC. Is HTTP2 (bandwidth)
 *   priority sufficient or do we also need to map onto thread priorities
 * - Have largely dispensed with the connection header and will either use hardcoded defaults or
 *   headers to replace it over time. Do need a GrpcSession though to cache authentication state.
 * - There is no useful monitoring or logging code in here currently, needs to be added back.
 * - Support for delayed calls removed.
 *
 *
 * @see RpcClientImpl
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class GrpcServer implements RpcServerInterface {
  private static final Log LOG = LogFactory.getLog(GrpcServer.class);

  // TODO(lryan): No security yet. TLS would be easy to enable but HBASE also needs SASL,
  // kerberos & simple.

  private final GrpcUtil grpcUtil;

  private ServiceAuthorizationManager authManager;

  /**
   * Keeps MonitoredRPCHandler per handler thread.
   */
  private static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC
      = new ThreadLocal<MonitoredRPCHandler>() {
    @Override
    protected MonitoredRPCHandler initialValue() {
      return new MonitoredRPCHandlerImpl();
    }
  };

  private final InetSocketAddress bindAddress;

  // TODO(lryan): Add back
  // protected MetricsHBaseServer metrics;

  private final Configuration conf;

  /**
   * This flag is used to indicate to sub threads when they should go down.  When we call {@link
   * #start()}, all threads started will consult this flag on whether they should keep going.  It is
   * set to false when {@link #stop()} is called.
   */
  private volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the call to {@link #start()}.
   */
  private volatile boolean started = false;

  /**
   * This is a running count of the size of all outstanding calls by size.
   */
  private final Counter callQueueSize = new Counter();


  private HBaseRPCErrorHandler errorHandler = null;

  private static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /**
   * Default value for above params
   */
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final int warnResponseTime;
  private final int warnResponseSize;
  private final Server server;

  private final io.grpc.Server grpcServer;

  private class CallListener extends ServerCall.Listener {
    private final RpcServer.BlockingServiceAndInterface service;
    private final MethodDescriptor methodDescriptor;
    private final ServerCall serverCall;
    private final ServerRpcController serverRpcController;

    public CallListener(RpcServer.BlockingServiceAndInterface service,
                        MethodDescriptor methodDescriptor,
                        ServerCall serverCall) {
      this.service = service;
      this.methodDescriptor = methodDescriptor;
      this.serverCall = serverCall;
      serverCall.request(1);
      serverRpcController = new ServerRpcController();
    }

    @Override
    public void onMessage(Object message) {
      try {
        Pair<Message, CellScanner> pair = (Pair<Message, CellScanner>) message;
        Pair<Message, CellScanner> response =
            GrpcServer.this.call(service.getBlockingService(), methodDescriptor, pair.getFirst(),
                pair.getSecond(), Long.MAX_VALUE /* FIXME(lryan) */, MONITORED_RPC.get());
        serverCall.sendHeaders(new Metadata());
        serverCall.sendMessage(response);
        serverCall.close(Status.OK, new Metadata());
      } catch (Exception se) {
        serverCall.close(Status.fromThrowable(se), new Metadata());
      }
    }

    @Override
    public void onHalfClose() {
      // No streaming yet so don't care.
    }

    @Override
    public void onCancel() {
      serverRpcController.startCancel();
    }

    @Override
    public void onComplete() {
      // No streaming yet so don't care.
    }
  }

  private static final Codec FIXME_CODEC = new KeyValueCodec();


  // TODO(lryan): This can all go away if we use the interfaces generated by the GRPC codegen instead of
  //
  @SuppressWarnings("unchecked")
  private ServerServiceDefinition bindService(
      final RpcServer.BlockingServiceAndInterface service) {
    Descriptors.ServiceDescriptor descriptor = service.getBlockingService().getDescriptorForType();
    ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(descriptor.getName());
    for (final MethodDescriptor methodDescriptor : descriptor.getMethods()) {
      builder.addMethod(
          fromProtoMethodDescriptor(methodDescriptor),
          new ServerCallHandler() {
            @Override
            public ServerCall.Listener startCall(io.grpc.MethodDescriptor grpcDescriptor,
                                                 final ServerCall serverCall,
                                                 Metadata headers) {
              // TODO(lryan): We' like to use headers instead of the connection prefix message
              // to communicate things like preferred codec etc which are currently hard-coded.
              return new CallListener(service, methodDescriptor, serverCall);
            }
          });
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private io.grpc.MethodDescriptor fromProtoMethodDescriptor(MethodDescriptor descriptor) {
    return io.grpc.MethodDescriptor.create(
        io.grpc.MethodDescriptor.MethodType.UNARY, // FIXME
        io.grpc.MethodDescriptor.generateFullMethodName(descriptor.getService().getName(),
            descriptor.getName()),
        grpcUtil.makePayloadAndScannerMarshaller(descriptor.getInputType()),
        grpcUtil.makePayloadAndScannerMarshaller(descriptor.getOutputType()));
  }

  /**
   * Constructs a server listening on the named port and address.
   * @param server hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param bindAddress Where to listen
   * @param conf
   */
  public GrpcServer(final Server server, final String name,
                   final List<RpcServer.BlockingServiceAndInterface> services,
                   final InetSocketAddress bindAddress, Configuration conf) throws Exception {
    grpcUtil = new GrpcUtil(new IPCUtil(conf), FIXME_CODEC);

    // Build the GRPC server
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder
        .forAddress(bindAddress)
        .executor(MoreExecutors.directExecutor())
        .flowControlWindow(1048576);
    for (RpcServer.BlockingServiceAndInterface service : services) {
      nettyServerBuilder.addService(bindService(service));
    }
    grpcServer = nettyServerBuilder.build();

    // TODO(lryan): Bring back the buffer resevoir?
    this.server = server;
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);

    //this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public synchronized void start() {
    if (started) return;
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    try {
      grpcServer.start();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    started = true;
  }

  @Override
  public void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  /**
   * This is a server side method, which is invoked over RPC. On success
   * the return response has protobuf response payload. On failure, the
   * exception name and the stack trace are returned in the protobuf response.
   */
  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
                                         Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
      throws IOException {
    try {
      status.setRPC(md.getName(), new Object[]{param}, receiveTime);
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      //get an instance of the method arg type
      long startTime = System.currentTimeMillis();
      PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cellScanner);
      Message result = service.callBlockingMethod(md, controller, param);
      long endTime = System.currentTimeMillis();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      //metrics.dequeuedCall(qTime);
      //metrics.processedCall(processingTime);
      //metrics.totalCall(totalTime);
      long responseSize = result.getSerializedSize();
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(new Object[]{param},
            md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
            (tooLarge ? "TooLarge" : "TooSlow"),
            status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<Message, CellScanner>(result, controller.cellScanner());
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE.  Strip the SE wrapper before
      // putting it on the wire.  Its needed to adhere to the pb Service Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) e = e.getCause();
      if (e instanceof LinkageError) throw new DoNotRetryIOException(e);
      if (e instanceof IOException) throw (IOException)e;
      LOG.error("Unexpected throwable object ", e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * @param params The parameters received in the call.
   * @param methodName The name of the method invoked
   * @param call The string representation of the call
   * @param tag  The tag that will be used to indicate this event in the log.
   * @param clientAddress   The address of the client who made this call.
   * @param startTime       The time that the call was initiated, in ms.
   * @param processingTime  The duration that the call took to run, in ms.
   * @param qTime           The duration that the call spent on the queue
   *                        prior to being initiated, in ms.
   * @param responseSize    The size in bytes of the response buffer.
   */
  void logResponse(Object[] params, String methodName, String call, String tag,
                   String clientAddress, long startTime, int processingTime, int qTime,
                   long responseSize)
      throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", server == null? "": server.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    if (params.length == 2 && server instanceof HRegionServer &&
        params[0] instanceof byte[] &&
        params[1] instanceof Operation) {
      // if the slow process is a query, we want to log its table as well
      // as its own fingerprint
      TableName tableName = TableName.valueOf(
          HRegionInfo.parseRegionName((byte[]) params[0])[0]);
      responseInfo.put("table", tableName.getNameAsString());
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[1]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
          MAPPER.writeValueAsString(responseInfo));
    } else if (params.length == 1 && server instanceof HRegionServer &&
        params[0] instanceof Operation) {
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[0]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
          MAPPER.writeValueAsString(responseInfo));
    } else {
      // can't get JSON details, so just report call.toString() along with
      // a more generic tag.
      responseInfo.put("call", call);
      LOG.warn("(response" + tag + "): " + MAPPER.writeValueAsString(responseInfo));
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server");
    running = false;
    // TODO(lryan): Does this need to block ?
    grpcServer.shutdown();
    notifyAll();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  @Override
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    return bindAddress;
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  @Override
  public MetricsHBaseServer getMetrics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addCallSize(final long diff) {
    this.callQueueSize.add(diff);
  }

  @Override
  public RpcScheduler getScheduler() {
    // TODO(lryan): Need to resolve how to handle RPC scheduling in the server. For the moment not
    // doing anything special in GRPC.
    throw new UnsupportedOperationException();
  }
}
