package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.reactivex.netty.client.CompositePoolLimitDeterminationStrategy;
import io.reactivex.netty.client.MaxConnectionsBasedStrategy;
import io.reactivex.netty.client.PoolLimitDeterminationStrategy;
import io.reactivex.netty.contexts.RxContexts;
import io.reactivex.netty.pipeline.PipelineConfigurator;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

public class RxNettyClientFactory implements Func1<HostAddress, Observable<HttpClient<ByteBuf, ByteBuf>>> {
    public static final PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> DEFAULT_HTTP_PIPELINE_CONFIGURATOR = 
            PipelineConfigurators.httpClientConfigurator();

    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    public static final int DEFAULT_READ_TIMEOUT = 2000;
    public static final boolean DEFAULT_FOLLOW_REDIRECTS = true;
    public static final boolean DEFAULT_CONNECTION_POOLING = true;
    public static final ScheduledExecutorService DEFAULT_SCHEDULER = Executors.newScheduledThreadPool(100);
    public static final MaxConnectionsBasedStrategy DEFAULT_MAX_CONNECTIONS_BASED_STRATEGY = new MaxConnectionsBasedStrategy(1000);
    public static final PoolLimitDeterminationStrategy DEFAULT_POOL_LIMIT_STRATEGY = new MaxConnectionsBasedStrategy(5000);
    public static final PoolLimitDeterminationStrategy DEFAULT_HOST_LIMIT_STRATEGY = new MaxConnectionsBasedStrategy(1000);
    public static final long DEFAULT_IDLE_CONNECTION_EVICTION_MILLIS = 60000L;
    
    public static class Builder {
        private PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> configurator = DEFAULT_HTTP_PIPELINE_CONFIGURATOR;
        private boolean followRedirects = DEFAULT_FOLLOW_REDIRECTS;
        private int readTimeout = DEFAULT_READ_TIMEOUT;
        private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private boolean connectionPooling = DEFAULT_CONNECTION_POOLING;
        private PoolLimitDeterminationStrategy poolStrategy = DEFAULT_POOL_LIMIT_STRATEGY;
        private long idleConnectionEvictionMills = DEFAULT_IDLE_CONNECTION_EVICTION_MILLIS;
        private ScheduledExecutorService poolCleanerScheduler = DEFAULT_SCHEDULER;

        public Builder withPipelineConfigurator(PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> configurator) {
            this.configurator = configurator;
            return this;
        }
        
        public Builder withConnectTimeout(int timeout) {
            this.connectTimeout = timeout;
            return this;
        }
        
        public Builder withReadTimeout(int timeout) {
            this.readTimeout = timeout;
            return this;
        }
        
        public Builder withFollowRedirects(boolean follow) {
            this.followRedirects = follow;
            return this;
        }
        
        public Builder withConnectionPooling(boolean enabled) {
            this.connectionPooling = enabled;
            return this;
        }
        
        public Builder withPoolLimitDeterminationStrategy(PoolLimitDeterminationStrategy poolStrategy) {
            this.poolStrategy = poolStrategy;
            return this;
        }
        
        public Builder withIdleConnectionEvictionMillis(long idleConnectionEvictionMills) {
            this.idleConnectionEvictionMills = idleConnectionEvictionMills;
            return this;
        }
        
        public Builder withScheduledExecutorService(ScheduledExecutorService poolCleanerScheduler) {
            this.poolCleanerScheduler = poolCleanerScheduler;
            return this;
        }

        public RxNettyClientFactory build() {
            return new RxNettyClientFactory(this);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    private final PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> configurator;
    private final int readTimeout;
    private final int connectTimeout;
    private final boolean followRedirect;
    private final boolean connectionPooling;
    private final PoolLimitDeterminationStrategy poolStrategy;
    private final long idleConnectionEvictionMills;
    private final ScheduledExecutorService poolCleanerScheduler;

    public RxNettyClientFactory(Builder builder) {
        this.configurator = builder.configurator;
        this.readTimeout = builder.readTimeout;
        this.connectTimeout = builder.connectTimeout;
        this.followRedirect = builder.followRedirects;
        this.connectionPooling = builder.connectionPooling;
        this.poolStrategy = builder.poolStrategy;
        this.idleConnectionEvictionMills = builder.idleConnectionEvictionMills;
        this.poolCleanerScheduler = builder.poolCleanerScheduler;
    }
    
    @Override
    public Observable<HttpClient<ByteBuf, ByteBuf>> call(final HostAddress server) {
        final long startTime = System.nanoTime();
        
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder()
            .readTimeout(readTimeout, TimeUnit.MILLISECONDS)
            .setFollowRedirect(followRedirect);
        
        HttpClientBuilder<ByteBuf, ByteBuf> clientBuilder;
        clientBuilder = RxContexts.<ByteBuf, ByteBuf>newHttpClientBuilder(
                server.getHost(),
                server.getPort(), 
                RxContexts.DEFAULT_CORRELATOR, 
                configurator)
              .channelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
              .config(builder.build());
        
        
        if (connectionPooling) {
            clientBuilder
              .withConnectionPoolLimitStrategy(new CompositePoolLimitDeterminationStrategy(DEFAULT_HOST_LIMIT_STRATEGY, DEFAULT_MAX_CONNECTIONS_BASED_STRATEGY))
              .withIdleConnectionsTimeoutMillis(idleConnectionEvictionMills)
              .withPoolIdleCleanupScheduler(poolCleanerScheduler);
        } 
        else {
            clientBuilder
              .withNoConnectionPooling();
        }
//      
//      if (sslContextFactory != null) {
//          try {
//              SSLEngineFactory myFactory = new DefaultFactories.SSLContextBasedFactory(sslContextFactory.getSSLContext()) {
//                  @Override
//                  public SSLEngine createSSLEngine(ByteBufAllocator allocator) {
//                      SSLEngine myEngine = super.createSSLEngine(allocator);
//                      myEngine.setUseClientMode(true);
//                      return myEngine;
//                  }
//              };
//  
//              clientBuilder.withSslEngineFactory(myFactory);
//          } catch (ClientSslSocketFactoryException e) {
//              throw new RuntimeException(e);
//          }
//      }
        try {
            return Observable.just(clientBuilder.build());
        }
        catch (Exception e) {
            return Observable.error(e);
        }
    }
}
