package netflix.ocelli.rxnetty;

import java.util.List;
import java.util.Map;

public class QueryParameters {
        
    private Map<String, List<String>> source;

    public QueryParameters(Map<String, List<String>> source) {
        this.source = source;
    }

    public static QueryParameters from(Map<String, List<String>> source) {
        return new QueryParameters(source);
    }
    
    public boolean exists(String key) {
        return source.containsKey(key);
    }
    
    public String firstAsString(String key) {
        List<String> values = source.get(key);
        if (values == null || values.isEmpty())
            return null;
        return values.get(0);
    }
    
    public String firstAsStringOrDefault(String key, String defaultValue) {
        List<String> values = source.get(key);
        if (values == null || values.isEmpty())
            return defaultValue;
        return values.get(0);
    }
    
    public Integer firstAsInt(String key) {
        List<String> values = source.get(key);
        if (values == null || values.isEmpty())
            return null;
        return Integer.parseInt(values.get(0));
    }
    
    public Integer firstAsIntOrDefault(String key, Integer defaultValue) {
        List<String> values = source.get(key);
        if (values == null || values.isEmpty())
            return defaultValue;
        return Integer.parseInt(values.get(0));
    }
    
    public List<String> get(String key) {
        return source.get(key);
    }
}
