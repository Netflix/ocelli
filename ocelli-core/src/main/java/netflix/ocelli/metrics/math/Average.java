package netflix.ocelli.metrics.math;


public interface Average {
    double get();
    void addSample(long sample);
    void reset();
}
