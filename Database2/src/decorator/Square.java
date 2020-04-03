package decorator;

public class Square extends Polygon {
	public Square(String name) {
		super(name);
		this.numSides = 4;
	}
	
	@Override
	public Triangle lowerOrder() {
		return new Triangle("tri");
	}
}
