package select_team_name;

import java.io.Serializable;

public class Point implements Comparable , Serializable {
		double x;
		double y;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		
	}
	
	public Point(int x, int y) {
		this.x = x;
		this.y = y;
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

	@Override
	public int compareTo(Object o) {
		Point p = (Point)o;
		if(this.x == p.x && this.y == p.y)
		return 0;
		else {
			return -1;
		}
	}
	
	
	
	
	
	

}
