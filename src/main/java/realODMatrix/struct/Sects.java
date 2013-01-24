package main.java.realODMatrix.struct;

import java.io.File;
import java.io.IOException;
//import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.geotools.data.FeatureSource;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.postgis.LinearRing;
import org.postgis.MultiPolygon;

public class Sects {
	private ArrayList<Sect> sects;
	public int sectCount;
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 */
	
	public Sects(String path) throws SQLException, IOException{
		this.sects = this.read(path);
		this.sectCount = this.sects.size();
	}
	
	private ArrayList<Sect> read(String path) throws SQLException, IOException{
		ArrayList<Sect> sectList = new ArrayList<Sect>();
		
		File file = new File(path);
		FileDataStoreFactorySpi factory = FileDataStoreFinder.getDataStoreFactory("shp");
		Map params = Collections.singletonMap( "url", file.toURL() );
		ShapefileDataStore shpDataStore = null;
		shpDataStore = new ShapefileDataStore(file.toURL());
		
		//Feature Access
		String typeName = shpDataStore.getTypeNames()[0];  
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = null;  
		featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>)shpDataStore.getFeatureSource(typeName);  
		FeatureCollection<SimpleFeatureType, SimpleFeature> result = featureSource.getFeatures();  
		FeatureIterator<SimpleFeature> itertor = result.features();  
		while(itertor.hasNext()){  
			//Data Reader
		    SimpleFeature feature = itertor.next();  
		    
		    //Fields Attributes
		    List fields = feature.getAttributes();
		    int ID = Integer.parseInt(feature.getAttribute("ID").toString());
		    
		    //Geo Attributes
			String geoStr = feature.getDefaultGeometry().toString();
			LinearRing linearRing = new MultiPolygon(geoStr).getPolygon(0).getRing(0);
			
			//Data import to sect
			Sect sect;
			ArrayList<Point> ps = new ArrayList<Point>();
			for (int idx = 0; idx < linearRing.numPoints(); idx++) {
				Point p = new Point(linearRing.getPoint(idx).x,linearRing.getPoint(idx).y);
				ps.add(p);
			}
			sect = new Sect(ps,ID);
			sectList.add(sect);
		}  
	    itertor.close();  
		return sectList;
	}

	public int fetchSect(Point p){
		int sectID = -1;
		for (Sect sect : this.sects) {
			if(sect.contains(p)){
				return sect.getID();
			}
		}
		return sectID;
	}
	
	public static void main(String[] args) throws SQLException, IOException {
		
		//Initializations
		//String path = "E:/datasource/sztb/dat/base/sects/Sects.shp";
		String path = "D:\\shenzhen GIS\\浜ら�灏忓尯鍒掑垎\\浜ら�灏忓尯鍒掑垎\\sects\\Sects.shp";
		Sects sects = new Sects(path);
		GPSRcrd record = new GPSRcrd(118716,32110,100,100);
		
		GPSRcrd record2 = new GPSRcrd(113.874794,22.558666,100,100);
		int id2 = sects.fetchSect(record2);
		System.out.println("GPSrecord2 113.874794,22.558666 falls into :"+id2);
		
		
		//Fetching id by Point location
		int id = sects.fetchSect(record);
		if(id==-1)
			System.out.println("no sects contain this record");
		else
			System.out.println("GPS Point falls into Sect No." + id);
		
		//Extra
		//Showing the geometric relationship
		Point p1 = new Point(0,0);
		Point p2 = new Point(1,0);
		Point p3 = new Point(1,1);
		Point p4 = new Point(0,1);
		Point tp1 = new Point(0.5,0.5);//inside
		Point tp2 = new Point(-10,0.5);//outside
		Point tp3 = new Point(1,0.5);//on the right edge
		Point tp4 = new Point(0,0.5);//on the left edge
		Point tp5 = new Point(0.5,1);//on the upper edge
		Point tp6 = new Point(0.5,0);//on the bottom edge
		ArrayList<Point> ps = new ArrayList<Point>();
		ps.add(p1);
		ps.add(p2);
		ps.add(p3);
		ps.add(p4);
		ps.add(p1);
		Polygon pg = new Polygon(ps);
		System.out.println(" contains:"+pg.contains(tp1));
		System.out.println(" intersect: "+pg.contains(tp2));
		System.out.println("intersect with righ boundary:"+pg.contains(tp3));
		System.out.println("intersect with left boundary:"+pg.contains(tp4));
		System.out.println("intersect with top boundary :"+pg.contains(tp5));
		System.out.println("intersect with bottom boundary: "+pg.contains(tp6));
		
		
		return;
	}
	
	
}
