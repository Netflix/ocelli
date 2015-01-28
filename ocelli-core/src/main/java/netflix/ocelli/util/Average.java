package netflix.ocelli.util;


public interface Average {
    double get();
    void addSample(long sample);
    void reset();
}
