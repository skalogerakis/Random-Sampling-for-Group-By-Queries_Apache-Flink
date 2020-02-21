package tuc;

import java.io.Serializable;

public class DataForSampling implements Serializable {

    String key;
    double mean;
    double sd;
    double count;
    double gamai;
    double gama;
    double si;

    public DataForSampling(String key, double mean, double sd, double count) {
        this.key = key;
        this.mean = mean;
        this.sd = sd;
        this.count = count;
    }

    public DataForSampling() {

    }



    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getSd() {
        return sd;
    }

    public void setSd(double sd) {
        this.sd = sd;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getGamai() {
        return gamai;
    }

    public void setGamai(double gamai) {
        this.gamai = gamai;
    }

    public double getGama() {
        return gama;
    }

    public void setGama(double gama) {
        this.gama = gama;
    }

    public double getSi() {
        return si;
    }

    public void setSi(double si) {
        this.si = si;
    }

}
