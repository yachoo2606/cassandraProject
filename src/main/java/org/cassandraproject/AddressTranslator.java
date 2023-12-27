package org.cassandraproject;

import com.datastax.driver.core.Cluster;

import java.net.InetSocketAddress;

public class AddressTranslator implements com.datastax.driver.core.policies.AddressTranslator {

    @Override
    public void init(Cluster cluster) {
    }

    @Override
    public InetSocketAddress translate(InetSocketAddress inetSocketAddress) {
        return switch (inetSocketAddress.getHostName()){
            case "172.23.0.2" -> new InetSocketAddress("localhost",9042);
            case "172.23.0.3" -> new InetSocketAddress("localhost",9043);
            case "172.23.0.4" -> new InetSocketAddress("localhost",9044);
            default -> throw new IllegalStateException("Unexpected value: " + inetSocketAddress.getHostName());
        };
    }

    @Override
    public void close() {

    }
}
