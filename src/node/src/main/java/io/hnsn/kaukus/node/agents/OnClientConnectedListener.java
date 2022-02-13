package io.hnsn.kaukus.node.agents;

import java.net.Socket;

public interface OnClientConnectedListener {
    void onConnected(Socket clientSocket);
}
