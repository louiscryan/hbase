/**
 *
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

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.grpc.stub.StreamObserver;

/**
 * TODO: http://go/java-style#javadoc
 */
@Category({ RPCTests.class, SmallTests.class })
public class TestGRPC extends AbstractTestIPC {

  private static final Log LOG = LogFactory.getLog(TestIPC.class);

  @Override
  protected GrpcClientImpl createRpcClientNoCodec(Configuration conf) {
    return new GrpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT) {
      @Override
      Codec getCodec() {
        return null;
      }
    };
  }

  @Override
  protected GrpcClientImpl createRpcClient(Configuration conf) {
    return new GrpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT);
  }

  @Override
  protected GrpcClientImpl createRpcClientRTEDuringConnectionSetup(Configuration conf)
      throws IOException {
    return new GrpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: TestIPC <CYCLES> <CELLS_PER_CYCLE>");
      return;
    }

    ServerSocket sock = new ServerSocket();
    sock.bind(new InetSocketAddress("localhost", 0));
    InetSocketAddress ephemeral = (InetSocketAddress) sock.getLocalSocketAddress();
    sock.close();


    // ((Log4JLogger)HBaseServer.LOG).getLogger().setLevel(Level.INFO);
    // ((Log4JLogger)HBaseClient.LOG).getLogger().setLevel(Level.INFO);
    int cycles = Integer.parseInt(args[0]);
    int cellcount = Integer.parseInt(args[1]);
    Configuration conf = HBaseConfiguration.create();
    TestGrpcServer rpcServer = new TestGrpcServer(ephemeral);
    final MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
    final EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
    final GrpcClientImpl client = new GrpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT);
    KeyValue kv = BIG_CELL;
    Put p = new Put(CellUtil.cloneRow(kv));
    for (int i = 0; i < cellcount; i++) {
      p.add(kv);
    }
    RowMutations rm = new RowMutations(CellUtil.cloneRow(kv));
    rm.add(p);
    try {
      rpcServer.start();
      final InetSocketAddress address = rpcServer.getListenerAddress();
      final User user = User.getCurrent();
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < cycles; i++) {
        List<CellScannable> cells = new ArrayList<CellScannable>();
        // Message param = RequestConverter.buildMultiRequest(HConstants.EMPTY_BYTE_ARRAY, rm);
        ClientProtos.RegionAction.Builder builder =
            RequestConverter.buildNoDataRegionAction(HConstants.EMPTY_BYTE_ARRAY, rm, cells,
                RegionAction.newBuilder(), ClientProtos.Action.newBuilder(),
                MutationProto.newBuilder());
        builder.setRegion(RegionSpecifier
            .newBuilder()
            .setType(RegionSpecifierType.REGION_NAME)
            .setValue(
                ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())));
        if (i % 100000 == 0) {
          LOG.info("" + i);
          // Uncomment this for a thread dump every so often.
          // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
          // "Thread dump " + Thread.currentThread().getName());
        }
        PayloadCarryingRpcController pcrc =
            new PayloadCarryingRpcController(CellUtil.createCellScanner(cells));
         client.call(pcrc, md, builder.build(), param, user, address);
      }
      LOG.info("Cycled " + cycles + " time(s) with " + cellcount + " cell(s) in "
          + (System.currentTimeMillis() - startTime) + "ms");

      startTime = System.currentTimeMillis();
      final CountDownLatch latch = new CountDownLatch(cycles);
      List<CellScannable> cells = new ArrayList<CellScannable>();
      final ClientProtos.RegionAction.Builder builder =
          RequestConverter.buildNoDataRegionAction(HConstants.EMPTY_BYTE_ARRAY, rm, cells,
              RegionAction.newBuilder(), ClientProtos.Action.newBuilder(),
              MutationProto.newBuilder());
      builder.setRegion(RegionSpecifier
          .newBuilder()
          .setType(RegionSpecifierType.REGION_NAME)
          .setValue(
              ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())));
      final PayloadCarryingRpcController pcrc =
          new PayloadCarryingRpcController(CellUtil.createCellScanner(cells));

      StreamObserver<Pair<Message, CellScanner>> observer =
          new StreamObserver<Pair<Message, CellScanner>>() {
        @Override
        public void onNext(Pair<Message, CellScanner> messageCellScannerPair) {

        }

        @Override
        public void onError(Throwable throwable) {
          latch.countDown();
        }

        @Override
        public void onCompleted() {
          latch.countDown();
          if (latch.getCount() > 0) {
            try {
              client.callAsync(pcrc, md, builder.build(), param, user, address, this);
            } catch (Throwable t) {
              // Ignore
            }
          }
        }
      };
      // Issue 100 calls concurrently
      for (int i = 0; i < 1000; i++) {
        client.callAsync(pcrc, md, builder.build(), param, user, address, observer);
      }
      latch.await();
      LOG.info("ASYNC 1000-concurrent : Cycled " + cycles + " time(s) with " + cellcount + " cell(s) in "
          + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      LOG.error("Error executing", e);
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  /**
   * Instance of server. We actually don't do anything speical in here so could just use
   * HBaseRpcServer directly.
   */
  static class TestGrpcServer extends GrpcServer {

    TestGrpcServer(InetSocketAddress address) throws Exception {
      super(null, "testRpcServer",
          Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
          address,
          CONF);
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
                                           Message param, CellScanner cellScanner,
                                           long receiveTime, MonitoredRPCHandler status)
        throws IOException {
      return super.call(service, md, param, cellScanner, receiveTime, status);
    }
  }


}
