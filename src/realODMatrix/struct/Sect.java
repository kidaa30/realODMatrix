package realODMatrix.struct;

import java.util.ArrayList;

public class Sect extends Polygon {
	
	private int id;
	/**
	 * @param args
	 */
	public Sect(ArrayList<Point> points,int id){
		this.points = points;
		this.id = id;
	}
	
	public int getID(){
		return this.id;
	}
}