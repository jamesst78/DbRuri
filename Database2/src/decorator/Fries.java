package decorator;

public class Fries {
	int count = 0;
	public Fries() {

	}
	public Fries(int count) {
		this.count = count;
	}
	public String taste() {
		return (count + " fries!");
	}
}
