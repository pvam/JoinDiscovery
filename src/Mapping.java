import java.text.DecimalFormat;
import java.util.ArrayList;

class Mapping implements Comparable<Mapping> {
    ArrayList<Integer> sourceList;
    ArrayList<Integer> targetList;
    double support;
    boolean frequent;

    public String toString() {
        String ret = "";
        ret+=  sourceList.toString() + " , " + targetList.toString()
                + " ,Support = " ;
        if (support <=1.0) {
            double scaledSupport = support *100;
            DecimalFormat dc = new DecimalFormat("#.##");
            ret += dc.format(scaledSupport) + "%";
        }
        else
            ret += "100% {Sanity bound}";
        return ret;
    }

    public int compare(Mapping a, Mapping b) {
        if (a.support >= b.support)
            return 1;
        else
            return 0;
    }

    @Override
    public int compareTo(Mapping o) {
        return Double.compare(o.support, support);
    }
}