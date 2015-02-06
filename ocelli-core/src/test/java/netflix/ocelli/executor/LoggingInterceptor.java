package netflix.ocelli.executor;

import netflix.ocelli.executor.InterceptingExecutor.Interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInterceptor<I, O> implements Interceptor<I, O> {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);
    
    @Override
    public I pre(I request) {
        LOG.info("Request : {}", request);
        return request;
    }

    @Override
    public O post(I request, O response) {
        LOG.info("Response : {} - {}", request, response);
        return response;
    }

    @Override
    public void error(I request, Throwable error) {
        LOG.error("Error processing request: {}", request.toString(), error);
    }
    
}
