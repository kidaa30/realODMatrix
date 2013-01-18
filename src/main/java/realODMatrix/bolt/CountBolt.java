/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 锟斤拷锟斤拷2:45:05
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.List;

import main.java.realODMatrix.spout.FieldListenerSpout;
import main.java.realODMatrix.spout.TupleInfo;


/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:45:05
 * email: gh.chen@siat.ac.cn
 *
 */
public class CountBolt implements IRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	double lanLast;   // last location of the vehicle
	double lonLast;
	Date dateTimeLast=null;
	int INTERVAL0 = 120; // We set time windows between two points 120 seconds;
	double DIST0=0.008993;  //  On the Earth, 1 Degree =111.2 km 
	                        //Distance between two points 1km, shoule be 1/111.2 =0.008993 Degree;
	
	private OutputCollector _collector;	
	Integer taskId;
	String taskName;
	Map<String, List<String> > districts;
	List<String> vehicleIdsInThisArea=new ArrayList<String>(); 
	Integer cnt;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.districts = new HashMap<String, List<String>>();
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
		this._collector=collector;		
	}

	
	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {
		
	     //FieldListenerSpout.writeToFile("/home/ghchen/output","CountBolt input:"+input.toString());

		List<String> gpsLineList=new ArrayList<String>();  // List one sequence of data: count,time,vehicleIdsInThisArea
		//vehicleIdsInThisArea.add("0");

        //List<Object>		countInput=input.getValues();
        
 
//        String districtID =countBoltInput[7].replace("]", "");
//		double lan= Double.parseDouble(countBoltInput[5]);
//		double lon= Double.parseDouble(countBoltInput[6]);
        
		String districtID =  input.getValues().get(7).toString();
		double lan= Double.parseDouble(input.getValues().get(5).toString());
		double lon= Double.parseDouble(input.getValues().get(6).toString());
        
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//Date dateTime = sdf.parse("2013-01-15 22:11:02");
		Date dateTime=null;
		try {
			dateTime = sdf.parse(input.getValues().get(1).toString());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}//new Date();
		//String time =sdf.format(dateTime);
		long  interval=0;
		double dist=Math.sqrt(Math.pow(lan-lanLast,2)+Math.pow(lon-lonLast,2));
		//FieldListenerSpout.writeToFile("/home/ghchen/output","DistrictID="+districtID+"lon="+lon+
				//"lan="+lan+"DateTime="+dateTime);
		
		if(!districts.containsKey(districtID)){	
			lonLast=lon;
			lanLast=lan;
			dateTimeLast=dateTime;
			cnt=1;

			if(null==input.getValues().get(0)){  // VEHICLE ID
				System.out.println("Error: Index 1 of input is NULL !");}
			else
				{
				//vehicleIdsInThisArea.remove(0);
				vehicleIdsInThisArea.add(input.getValues().get(0).toString());}
			
			
			gpsLineList.add(districtID);
			gpsLineList.add(cnt.toString());
			gpsLineList.add(input.getValues().get(1).toString());	// get Time stamp from input	
			gpsLineList.addAll(vehicleIdsInThisArea);  				
			
			districts.put(districtID, gpsLineList);
		}
		 // convert dateTime form string to class Date	
		interval=(dateTime.getTime()-dateTimeLast.getTime())/1000; 
		

		/** If the word dosn't exist in the map we will create
		 * this, if not We will create a new thread and  add 1 */		
		if(dist>DIST0 && interval>INTERVAL0){
			if(districts.containsKey(districtID)){	
				cnt =Integer.parseInt(districts.get(districtID).get(1)) + 1;
				//vehicleIdsInThisArea.remove(0);
				vehicleIdsInThisArea.add((String) input.getValues().get(1));

				gpsLineList.add(districtID);
				gpsLineList.add(cnt.toString());
				gpsLineList.add(input.getValues().get(1).toString());	// get Time stamp from input	
				gpsLineList.addAll(vehicleIdsInThisArea);  

				districts.put(districtID, gpsLineList);	

			}		
    		lanLast = lan;
			lonLast = lon;
			
			try {
				dateTimeLast=sdf.parse(input.getValues().get(1).toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			FieldListenerSpout.writeToFile("/home/ghchen/vehicleIdsInThisArea","vehicleIdsInThisArea:"+vehicleIdsInThisArea.toString());
			FieldListenerSpout.writeToFile("/home/ghchen/districts","CountBolt districts:"+districts.toString());
		_collector.emit(new Values(districts));

		}

		
		Date nowtime=new Date();
		int  timeMinute= nowtime.getMinutes();	
		
		/* Every ten minute, we reset the list to null;	 * */		
		if(0==(timeMinute%10)){   //every 10 minutes
			cnt=0;
			vehicleIdsInThisArea=null;
		}			
		_collector.ack(input);
	}

	
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+taskName+"-"+taskId+"] --");
		for(Map.Entry<String, List<String> > entry : districts.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
		
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("districts"));
	}

	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
