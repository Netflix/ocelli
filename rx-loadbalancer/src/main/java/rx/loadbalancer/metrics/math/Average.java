package rx.loadbalancer.metrics.math;


public interface Average {
    double get();
    void addSample(int sample);
    void reset();
}
