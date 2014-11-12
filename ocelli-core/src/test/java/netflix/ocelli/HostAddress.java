package netflix.ocelli;

import java.net.URI;
import java.net.URISyntaxException;

public class HostAddress {
    private String host;
    private int    port;
    private String rack;
    private String dc;
    
    public static HostAddress from(String url) {
        URI uri;
        try {
            uri = new URI(url);
            return new HostAddress().setHost(uri.getHost()).setPort(uri.getPort());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static HostAddress from(String host, int port) {
        return new HostAddress().setHost(host).setPort(port);
    }
    
    public static HostAddress from(String rack, String host, int port) {
        return new HostAddress().setRack(rack).setHost(host).setPort(port);
    }
    
    public String getHost() {
        return host;
    }
    
    public HostAddress setHost(String host) {
        this.host = host;
        return this;
    }
    
    public int getPort() {
        return port;
    }
    
    public HostAddress setPort(int port) {
        this.port = port;
        return this;
    }
    
    /**
     * AKA Ec2 zone
     * 
     * Rack is always lower case
     * @return
     */
    public String getRack() {
        return rack;
    }
    
    public HostAddress setRack(String rack) {
        this.rack = rack.toLowerCase();
        return this;
    }
    
    public String getDc() {
        return dc;
    }
    
    /**
     * AKA Ec2 region
     * @param dc
     * @return
     */
    public HostAddress setDc(String dc) {
        this.dc = dc;
        return this;
    }
    
    public String getHostPort() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dc == null) ? 0 : dc.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        result = prime * result + ((rack == null) ? 0 : rack.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostAddress other = (HostAddress) obj;
        if (dc == null) {
            if (other.dc != null)
                return false;
        } else if (!dc.equals(other.dc))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        if (rack == null) {
            if (other.rack != null)
                return false;
        } else if (!rack.equals(other.rack))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Server [");
        if (dc != null) {
            sb.append(dc).append(":");
        }
        if (rack != null) {
            sb.append(rack).append(":");
        }
        sb.append(host)
          .append(":")
          .append(port)
          .append("]");
        return sb.toString();
    }
}
