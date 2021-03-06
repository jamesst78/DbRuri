package select_team_name;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.StringTokenizer;





public class Polygon extends java.awt.Polygon implements Comparable {
	ArrayList<Point> points ;
	
	 public Polygon(int[]x,int[]y,int z){
		 super(x,y,z);
		 points = new ArrayList<>();
		 for(int i = 0 ; i<x.length ; i++) {
			 points.add(new Point(x[i] , y[i]));
		 }
	 }

	public static Polygon parsePolygon(String s) throws DBAppException {
		int c = 0;
		String s2 = "";
		for(int i = 0;i<s.length();i++) {
			if(s.charAt(i)=='(')
				c++;
			else if(s.charAt(i)==')')
				c--;
			else
				s2+= s.charAt(i);
			if(c!=0 &&c!=1) {
				throw new DBAppException("Incorrect polygon format");
			}
		}
		StringTokenizer st = new StringTokenizer(s2, ",");
		if(st.countTokens()%2==1) {
			throw new DBAppException("Incorrect polygon format");
		}
		int[] xp = new int[st.countTokens()/2];
		int[] yp = new int[st.countTokens()/2];
		int i = 0;
		while(st.hasMoreTokens()) {
			xp[i] = Integer.parseInt(st.nextToken());
			yp[i] = Integer.parseInt(st.nextToken());
			i++;
		}
		Polygon p = new Polygon(xp, yp, xp.length);
		return p;
	}
	@Override
	public int compareTo(Object arg0) {
		Polygon p2 = (Polygon) arg0;
		Dimension dim = p2.getBounds( ).getSize( );
		int ThisArea = dim.width * dim.height;
		Dimension dim1 = this.getBounds( ).getSize( );
		int ThisArea1 = dim1.width * dim1.height;
		return ThisArea1-ThisArea ;
	} 
	
	public float [][] polygonToRectanglePoints() {
		float[][] points = new float [2][2];
		Rectangle r = this.getBoundingBox();
		float minX = (float) r.getMinX();
		float maxX = (float) r.getMaxX();
		float minY = (float) r.getMinY();
		float maxY = (float) r.getMaxY();
		points[0][0] = minX;
		points[0][1] = minY;
		points[1][0] = maxX-minX;
		points[1][1] = maxY-minY;   //hi ya ruri
		
		return points;
		
	}
	
	public double getArea() {
		Dimension dim1 = this.getBounds( ).getSize( );
		double ThisArea1 = dim1.width * dim1.height;
		return ThisArea1;
	}
	
	public boolean uniqueEqual(Polygon P) {
		
		Point testPoint = null;
		boolean exactPolygon = true;
		boolean pointFound = false;
		
		ArrayList<Boolean> pointsFound = new ArrayList<>();
		
		for(int i = 0 ; i<this.points.size() ; i++) {
			testPoint = this.points.get(i);
				for(int j = 0 ; j<P.points.size() ; j++) {
					if(testPoint.compareTo(P.points.get(j)) == 0) {
						pointFound = true;
					
					}
					
				}
			//	System.out.println("the boolean for point: "+pointFound);
				pointsFound.add(pointFound);
				pointFound = false;
			
		}
		
		for(int k = 0 ; k<pointsFound.size() ; k++) {
			if(pointsFound.get(k) == false) {
				return false;
			}
			
		}
		return true;
		
	}
	

	
	
	public static void main(String[] args) throws DBAppException{
		Polygon p = parsePolygon("(0,3),(5,7),(2,2)");
			
		p.polygonToRectanglePoints();
		
		
	}
	
	
}