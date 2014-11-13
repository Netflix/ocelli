package netflix.ocelli;

/**
 * A class for expressing a host.
 *
 * @author Nitesh Kant
 */
public class Host {

    private String hostName;
    private int port;

    public Host(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
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
}
