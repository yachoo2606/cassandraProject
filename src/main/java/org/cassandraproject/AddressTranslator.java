package org.cassandraproject;

import com.datastax.driver.core.Cluster;
import lombok.Getter;
import lombok.Setter;

import java.net.InetSocketAddress;
import java.util.Properties;

@Getter
@Setter
public class AddressTranslator implements com.datastax.driver.core.policies.AddressTranslator {
    private Properties properties;

    public AddressTranslator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void init(Cluster cluster) {
    }

    @Override
    public InetSocketAddress translate(InetSocketAddress inetSocketAddress) {
        return switch (inetSocketAddress.getHostName()){
            case "172.23.0.2" -> new InetSocketAddress(properties.getProperty("server.address_one"), Integer.parseInt(properties.getProperty("server.port")));
            case "172.23.0.3" -> new InetSocketAddress(properties.getProperty("server.address_two"), Integer.parseInt(properties.getProperty("server.port2")));
            case "172.23.0.4" -> new InetSocketAddress(properties.getProperty("server.address_three"), Integer.parseInt(properties.getProperty("server.port3")));
            default -> throw new IllegalStateException("Unexpected value: " + inetSocketAddress.getHostName());
        };
    }

    @Override
    public void close() {

    }
}
