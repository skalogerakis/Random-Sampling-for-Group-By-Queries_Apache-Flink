package tuc;

import org.apache.flink.api.java.tuple.Tuple11;

import java.io.Serializable;
import java.util.Vector;

public class StateSample implements Serializable {

    int count;
    Vector <Tuple11>sample;

    public StateSample() {
        sample =new Vector<>();
    }





    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Vector<Tuple11> getSample() {
        return sample;
    }

    public void setSample(Vector<Tuple11> sample) {
        this.sample = sample;
    }



}
