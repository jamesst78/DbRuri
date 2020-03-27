package select_team_name;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.StringTokenizer;

import com.github.davidmoten.rtree.geometry.Geometries;



public class Polygon extends java.awt.Polygon implements Comparable {
	 public Polygon(int[]x,int[]y,int z){
		 super(x,y,z);
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
	
	public Double [] polygonToRectanglePoints() {
		Double[] points = new Double [4];
		Rectangle r = this.getBoundingBox();
		double minX = r.getMinX();
		double maxX = r.getMaxX();
		double minY = r.getMinY();
		double maxY = r.getMaxY();
		points[0] = minX;
		points[1] = minY;
		points[2] = maxX;
		points[3] = maxY;
		for(int i = 0 ; i< points.length ; i++) {
		System.out.println(points[i]);
		}
		return points;
		
	}
	
	
	public static void main(String[] args) throws DBAppException{
		Polygon p = parsePolygon("(0,3),(5,7),(2,2)");
			
		p.polygonToRectanglePoints();
		
		
	}
	
	
}