package decorator;

public class Test {
	public static void main(String[] args) {
		Fries f = new Fries(30);
		f = new FriesWithSalt(f);
		f = new FriesWithSalt(f);
		System.out.println(f.taste());
	}
}
