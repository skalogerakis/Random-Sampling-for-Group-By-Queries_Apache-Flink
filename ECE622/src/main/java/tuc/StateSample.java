package tuc;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Vector;

public class StateSample implements Serializable {

    public int count;
    public Vector <Tuple2>sample;

    public StateSample() {
        sample =new Vector<>();

    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Vector<Tuple2> getSample() {
        return sample;
    }

    public void setSample(Vector<Tuple2> sample) {
        this.sample = sample;
    }



}
