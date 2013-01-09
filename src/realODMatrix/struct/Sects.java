package realODMatrix.struct;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
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
import org.postgis.LineString;
import org.postgis.LinearRing;
import org.postgis.MultiLineString;
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
			if(sect.contains(p))
				return sect.getID();
		}
		return sectID;
	}
	
	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		String path = "E:/datasource/sztb/dat/base/sects/Sects.shp";
		Sects sects = new Sects(path);
		
		return;
	}
	
	
}
