/**
 * realODMatrix realODMatrix.bolt DistrictMatchingBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Author: admin
 * Last Updated:2013-1-8 锟斤拷锟斤拷2:39:14
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
//import backtype.storm.timer.schedule_recurring.this__1458;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import main.java.realODMatrix.struct.*;

/**
 * realODMatrix realODMatrix.bolt DistrictMatchingBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:39:14
 * email: gh.chen@siat.ac.cn 
 */
public class DistrictMatchingBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	
	Integer districtID ;
	GPSRcrd record;
	Map<GPSRcrd, Integer> gpsMatch;  //map<GPSRcrd,districtID>
	Integer taskID;
	String name;
	


	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector=collector;	
		this.taskID=context.getThisTaskId();
		this.name=context.getThisComponentId();
		
	}

	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//String path = "E:/datasource/sztb/dat/base/sects/Sects.shp";
		String path = "/home/ghchen/sects/sects.shp";
		Sects sects;
		try {
			sects = new Sects(path);


			Fields inputLine = input.getFields();

				record=new GPSRcrd(Double.parseDouble(inputLine.get(6)), 
					Double.parseDouble(inputLine.get(5)), Integer.parseInt(inputLine.get(3)), 
					Integer.parseInt(inputLine.get(4)));


			districtID = sects.fetchSect(record);
			
			if(taskID==-1)
				System.out.println("no sects contain this record");
			else
			{
				System.out.println("GPS Point falls into Sect No. :" + districtID);
				//gpsMatch.put(record, 1);
			}
			
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}

	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
		System.out.println("-- District Mathchier ["+name+"-"+districtID+"] --");
		//for(Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()){
		//System.out.println(entry.getKey()+": "+entry.getValue());
		//}
		
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		
		
		
		
	}

	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
