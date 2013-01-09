package realODMatrix.struct;

import java.util.ArrayList;

/**
 * @author Jags
 *
 */
public class Polygon {
	public ArrayList<Point> points;
	public double xmin;
	public double xmax;
	public double ymin;
	public double ymax;
	public int count;
	
	public Polygon(){
		this.points = new ArrayList<Point>();
	}
	public Polygon(ArrayList<Point> points){
		this.points = points;
		this.count = points.size();
		
		this.xmin = Double.MAX_VALUE;
		this.xmax = Double.MIN_VALUE;
		this.ymin = Double.MAX_VALUE;
		this.ymax = Double.MIN_VALUE;
		for (Point p : points) {
			if(p.x>this.xmax)
				this.xmax = p.x;
			if(p.x<this.xmin)
				this.xmin = p.x;
			if(p.y>this.ymax)
				this.ymax = p.y;
			if(p.y<this.ymin)
				this.ymin = p.y;
		}	
	}
	
	public Boolean contains(Point p){
		//
		if(p.x>=xmax||p.x<=xmin||p.y>=ymax||p.y<=ymin)
			return false;
		
		int n = points.size();
		for (int i = 0; i < n - 1; i++) {
			if(points.get(i).y!=points.get(i+1).y){//erase the condition of horizonal line
					
			}
		}
		
		
		int cn = 0 ; //the crossing number counter
		
		
		
		return false;
	}
	
	
	
}
