package org.apache.hadoop.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by antonis on 11/21/16.
 */
public interface RpcSSLEngine {
    boolean doHandshake() throws IOException;
    int decryptData(ReadableByteChannel channel, ByteBuffer buffer) throws IOException;
    int write(WritableByteChannel channel, ByteBuffer src) throws IOException;
}
