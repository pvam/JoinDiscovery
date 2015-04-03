public class Pair implements Comparable<Pair> {
	public String src;
	public String dest;
	public double support;

	Pair(String S, String D, double sup) {
		src = S;
		dest = D;
		support = sup;
	}

	@Override
	public int compareTo(Pair n) {
		return Double.compare(n.support, support);
	}

	@Override
	public String toString() {
		return "(" + src + "," + dest + ") =>" + support;
	}
}
