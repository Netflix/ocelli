package netflix.ocelli;

import java.util.Map;

/**
 * A class for expressing a host.
 *
 * @author Nitesh Kant
 */
public class Host {

    private String hostName;
    private int port;
    private Map<String, String> metadata;

    public Host(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
    }

    public Host(String hostName, int port, Map<String, String> metadata) {
        this.hostName = hostName;
        this.port = port;
        this.metadata = metadata;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Host)) {
            return false;
        }

        Host host = (Host) o;

        return port == host.port && !(hostName != null ? !hostName.equals(host.hostName) : host.hostName != null);
    }

    @Override
    public int hashCode() {
        int result = hostName != null ? hostName.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }
    
    @Override
    public String toString() {
        return "Host [hostName=" + hostName + ", port=" + port + ", metadata="
                + metadata + "]";
    }
}
