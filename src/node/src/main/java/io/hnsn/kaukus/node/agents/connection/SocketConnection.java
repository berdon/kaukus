package io.hnsn.kaukus.node.agents.connection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.streams.NonClosingStream;
import io.hnsn.kaukus.streams.RangedInputStream;
import lombok.Getter;

public abstract class SocketConnection extends BaseConnection {
    private static final int HEADER_SIZE = 4;
    private final Logger log;
    private final ConnectionCloseable closeable;
    @Getter
    private final Socket socket;
    private final Thread messageHandler;
    private final byte[] sendHeaderBuffer = new byte[HEADER_SIZE];
    private final byte[] receiveHeaderBuffer = new byte[HEADER_SIZE];

    private OutputStream outputStream;
    private boolean isClosed = false;
    private boolean isSocketDetectedClosed = false;
    private AtomicBoolean isClosing = new AtomicBoolean(false);

    public SocketConnection(LoggerProvider loggerProvider, ConnectionCloseable closeable, Socket socket) {
        super(loggerProvider);
        this.log = loggerProvider.get("SocketConnection");
        this.closeable = closeable;
        this.socket = socket;
        messageHandler = new Thread(this::messageLooper);
        messageHandler.start();
    }

    @Override
    public synchronized void sendMessage(byte[] buffer) throws IOException {
        log.trace("Sending message of length {}", buffer.length);
        if (this.outputStream == null) {
            this.outputStream = new NonClosingStream(socket.getOutputStream());
        }

        try (var bufferedOutputStream = new BufferedOutputStream(this.outputStream)) {
            // Send the header
            sendHeaderBuffer[0] = (byte) (buffer.length & 0xFF);
            sendHeaderBuffer[1] = (byte) ((buffer.length >>> 8) & 0xFF);
            sendHeaderBuffer[2] = (byte) ((buffer.length >>> 16) & 0xFF);
            sendHeaderBuffer[3] = (byte) ((buffer.length >>> 24) & 0xFF);
            bufferedOutputStream.write(sendHeaderBuffer);
            bufferedOutputStream.write(buffer);
            bufferedOutputStream.flush();
        }
        catch(Exception e) {
            log.error("", e);
        }
    }

    private void messageLooper() {
        try {
            var socket = getSocket();

            try (var inputStream = new BufferedInputStream(socket.getInputStream())) {
                while (!isClosed()) {
                    var headerLength = 0;
                    var payloadLength = 0;

                    while (!isClosed() && headerLength != HEADER_SIZE) {
                        var count = inputStream.read(receiveHeaderBuffer, headerLength, HEADER_SIZE - headerLength);
                        if (count == -1) {
                            isSocketDetectedClosed = true;
                            break;
                        }
                        else {
                            headerLength += count;
                        }
                    }
                    if (isClosed()) {
                        log.debug("isClosed() == true");
                        break;
                    }
                    if (headerLength != HEADER_SIZE) {
                        log.warn("Socket/InputStream closed while reading header; only read {} bytes", headerLength);
                        break;
                    }

                    var expectedPayloadLength = ((receiveHeaderBuffer[3] & 0xFF) << 24) |
                                                ((receiveHeaderBuffer[2] & 0xFF) << 16) |
                                                ((receiveHeaderBuffer[1] & 0xFF) <<  8) |
                                                (receiveHeaderBuffer[0] & 0xFF);

                    log.trace("Received message header indicating payload length of {}", expectedPayloadLength);

                    // Debugging
//                     var payloadBuffer = new byte[expectedPayloadLength];
//                     while (!isClosed() && payloadLength != expectedPayloadLength) {
//                         var count = inputStream.read(payloadBuffer, payloadLength, expectedPayloadLength - payloadLength);
//                         if (count == -1) {
//                             isSocketDetectedClosed = true;
//                             break;
//                         }
//                         else {
//                             payloadLength += count;
//                         }
//                     }
//                     if (isClosed()) {
//                         log.debug("isClosed() == true");
//                         break;
//                     }
//                     if (payloadLength != expectedPayloadLength) {
//                         log.warn("Socket/InputStream closed while reading payload; only read {} bytes but expected {}", payloadLength, expectedPayloadLength);
//                         break;
//                     }
//                     var newInputStream = new BufferedInputStream(new ByteArrayInputStream(payloadBuffer));

                    // new DataReader
                    var rangedInputStream = new RangedInputStream(inputStream, expectedPayloadLength);
                    var datumReader = new GenericDatumReader<GenericRecord>();
                    try (var dataReader = new DataFileStream<>(rangedInputStream, datumReader)) {
                        var payload = dataReader.next();
                        onMessageReceived(payload);
                    } catch (Exception e) {
                        log.warn("Exception occurred while parsing packet", e);
                    }
                }
            }
        }
        catch (IOException e) {
            // TODO
            log.warn("", e);
        }

        try {
            close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.warn("Error while closing after exception", e);
        }
    }

    public boolean isClosed() {
        return isClosed || isSocketDetectedClosed || socket.isClosed();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed && isClosing.compareAndSet(false, true)) {
            messageHandler.interrupt();
            closeable.close(this);
            isClosed = true;

            onClosed();
        }
    }
}
