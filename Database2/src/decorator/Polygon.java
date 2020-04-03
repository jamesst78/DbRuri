package decorator;

public class Polygon {
	String name;
	int numSides;
	
	public Polygon() {
		this.name = "Default";
	}
	
	public Polygon(String name) {
		this.name = name;
	}
	
	public Polygon lowerOrder() {
		return new Polygon();
	}
}
