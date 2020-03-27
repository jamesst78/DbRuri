package select_team_name;

public class Point {
		double x;
		double y;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		
	}
	
	public Point getMinX(Point p) {
		if(this.x < p.x) {
			return this;
		}
		else
			return p;
	}
	public Point getMaxX(Point p) {
		if(this.x > p.x) {
			return this;
		}
		else
			return p;
	}
	
	public Point getMaxY(Point p) {
		if(this.y > p.y) {
			return this;
		}
		else
			return p;
	}
	public Point getMinY(Point p) {
		if(this.y < p.y) {
			return this;
		}
		else
			return p;
	}
	
	
	
	

}
