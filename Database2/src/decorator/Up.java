package decorator;

public class Up {
	public Comparable doIt() {
		return new Comparable<Integer>() {
			@Override
			public int compareTo(Integer arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		};
	}
}
