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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import io.grpc.Drainable;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;

/**
 * Utility functions for GRPC. Handles specialized bridging to old generated proto RPC
 * interfaces as well as handling the Pair<Message, CellScanner> pattern.
 */
public class GrpcUtil {

  private final IPCUtil ipcUtil;
  private final Codec codec;
  private final IdentityHashMap<Object,
      MethodDescriptor<Pair<Message, CellScanner>, Pair<Message, CellScanner>>> methodCache;

  GrpcUtil(IPCUtil ipcUtil, Codec codec) {
    this.ipcUtil = ipcUtil;
    this.codec = codec;
    this.methodCache = new IdentityHashMap<>();
  }

  /**
   * Resolve the message class for a message descriptor.
   */
  static Class resolve(Descriptors.Descriptor messageDescriptor) throws Exception {
    DescriptorProtos.FileOptions fileOptions = messageDescriptor.getFile().getOptions();
    String className = fileOptions.getJavaPackage()
        + "."
        + fileOptions.getJavaOuterClassname()
        + "$"
        + messageDescriptor.getName();
    return Class.forName(className);
  }

  /**
   * Produce a GRPC client {@link MethodDescriptor} for the given proto
   * {@link com.google.protobuf.Descriptors.MethodDescriptor}. The generated descriptor
   * uses a custom {@link io.grpc.MethodDescriptor.Marshaller} that appends an optional
   * {@link CellScanner} on the wire.
   */
  MethodDescriptor<Pair<Message, CellScanner>, Pair<Message, CellScanner>>
      getMethodDesriptor(Descriptors.MethodDescriptor protoMethod, long timeoutMillis) {
    // TODO(lryan): This seems to be coming from conf as 0 so need to make this assumption.
    if (timeoutMillis == 0) {
      timeoutMillis = Long.MAX_VALUE;
    }
    MethodDescriptor<Pair<Message, CellScanner>, Pair<Message, CellScanner>>  methodDescriptor;
    synchronized (methodCache) {
      methodDescriptor = methodCache.get(protoMethod);
      if (methodDescriptor == null) {
        methodDescriptor = MethodDescriptor.create(MethodDescriptor.MethodType.UNARY,
            protoMethod.getService().getFullName() + "/" + protoMethod.getName(),
            makePayloadAndScannerMarshaller(protoMethod.getInputType()),
            makePayloadAndScannerMarshaller(protoMethod.getOutputType()));
        methodCache.put(protoMethod, methodDescriptor);
      }
    }
    return methodDescriptor;
  }

  /**
   * Create a GRPC {@link Marshaller} for the given protobuf message type.
   */
  Marshaller<Pair<Message, CellScanner>> makePayloadAndScannerMarshaller(
      Descriptors.Descriptor descriptor) {
    return new PayloadAndScannerMarshaller(descriptor);
  }


  // TODO(lryan): There is some unnecessary buffer copying that can be optimized away but
  // this will do to demonstrate the encoding works. Ideally we would have the
  // cellScanner written directly to a Netty buffer which the GRPC framer would just
  // pass through to flow-control.
  private class PayloadAndScannerMarshaller implements Marshaller<Pair<Message, CellScanner>> {

    private final Marshaller protoMarshaller;

    private PayloadAndScannerMarshaller(Descriptors.Descriptor descriptor) {
      try {
        // Extract the generated proto PARSER object.
        Class<?> protoClass = resolve(descriptor);
        Method method = protoClass.getMethod("getDefaultInstance");
        MessageLite defaultInstance = (MessageLite) method.invoke(null);
        this.protoMarshaller = ProtoUtils.marshaller(defaultInstance);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputStream stream(final Pair<Message, CellScanner> pair) {
      // Use deferred output stream so we don't have to buffer excessively
      return new PayloadAndScannerDeferredInputStream(pair);
    }

    @Override
    public Pair<Message, CellScanner> parse(InputStream inputStream) {
      try {
        int first = inputStream.read();
        int messageSize = CodedInputStream.readRawVarint32(first, inputStream);
        // TODO(lryan): Could use mergeFromDelimited here to avoid constructing LimitInputStream.
        Message message = (Message) protoMarshaller.parse(
            new LimitInputStream(inputStream, messageSize));
        CellScanner scanner = null;
        if (inputStream.available() > 0) {
          // Just need the codec, let GRPC handle compression.
          scanner = codec.getDecoder(inputStream);
        }
        return new Pair<>(message, scanner);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    /**
     * Implementation of {@link io.grpc.Drainable} which can serialize a
     * pair of message & optional cell scanner.
     */
    private class PayloadAndScannerDeferredInputStream extends InputStream implements Drainable {
      private Pair<Message, CellScanner> pair;

      public PayloadAndScannerDeferredInputStream(Pair<Message, CellScanner> pair) {
        this.pair = pair;
      }

      @Override
      public int drainTo(OutputStream outputStream) throws IOException {
        if (pair == null) {
          // contents already written
          return 0;
        }
        pair.getFirst().writeDelimitedTo(outputStream);
        ipcUtil.buildCellBlockToStream(codec, null, pair.getSecond(), outputStream);
        pair = null;
        // Don't need to return accurate length as we do not implement KnownLength
        return -1;
      }

      @Override
      public int read() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int available() throws IOException {
        return -1;
      }
    }
  }
}
