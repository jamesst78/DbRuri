package decorator;

public class FriesWithSalt extends Fries {
	private Fries fr;
	public FriesWithSalt(Fries fr) {
		this.fr = fr;
	}
	public String taste() {
		return fr.taste() + " With salt!";
	}
}
