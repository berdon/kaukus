package io.hnsn.kaukus.node.agents;

import java.net.DatagramPacket;

public interface OnBroadcastReceivedListener {
    void onReceived(DatagramPacket packet);
}
