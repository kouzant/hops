package org.apache.hadoop.ipc;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by antonis on 11/21/16.
 */
public interface RpcSSLEngine {
    boolean doHandshake() throws IOException;
    SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException;
    SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException;
    void closeOutbound();
    int decryptData(ReadableByteChannel channel, ByteBuffer buffer) throws IOException;
    int read(ReadableByteChannel channel, ByteBuffer dst) throws IOException;
    int write(WritableByteChannel channel, ByteBuffer src) throws IOException;
}
