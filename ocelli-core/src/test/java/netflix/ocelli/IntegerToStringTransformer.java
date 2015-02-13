package netflix.ocelli;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Action1;
import rx.functions.Func1;

public class IntegerToStringTransformer extends CachingInstanceTransformer<Integer, String> {
    private final static Logger LOG = LoggerFactory.getLogger(IntegerToStringTransformer.class);
    
    private final CopyOnWriteArrayList<String> added ;
    private final CopyOnWriteArrayList<String> removed;
    
    public IntegerToStringTransformer() {
        this(new CopyOnWriteArrayList<String>(), new CopyOnWriteArrayList<String>());
    }
    
    private IntegerToStringTransformer(final CopyOnWriteArrayList<String> added, final CopyOnWriteArrayList<String> removed) {
        super(
            new Func1<Integer, String>() {
                @Override
                public String call(Integer host) {
                    String client = "Client-" + host;
                    added.add(client);
                    return client;
                }
            },
            new Action1<String>() {
                @Override
                public void call(String client) {
                    removed.add(client);
                    LOG.info("Destroy {}", client);
                }
            },
            new Func1<String, Instance<String>>() {
                @Override
                public Instance<String> call(String t1) {
                    return MutableInstance.from(t1);
                }
            });
        
        this.added = added;
        this.removed = removed;
    }
    
    public List<String> removed() {
        return removed;
    }
    
    public List<String> added() {
        return added;
    }
}
