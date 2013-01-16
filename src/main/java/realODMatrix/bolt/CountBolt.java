/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 锟斤拷锟斤拷2:45:05
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

//import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

import java.util.List;

import javax.measure.quantity.Power;

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
	
	double lanLast;   // last location of the vehicle
	double lonLast;
	Date dateTimeLast=null;
	int INTERVAL0 = 180; // We set time windows between two points 180 seconds;
	double DIST0=0.008993;  //  On the Earth, 1 Degree =111.2 km 
	                        //Distance between two points 1km, shoule be 1/111.2 =0.008993 Degree;
	
	private OutputCollector _collector;	
	Integer taskId;
	String taskName;
	Map<String, List<String> > districts;
	List<String> vehicleIdsInThisArea; 
	Integer cnt;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.districts = new HashMap<String, List<String>>();
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
				
	}

	
	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {
		
	     FieldListenerSpout.writeToFile("/home/ghchen/output","CountBolt input:"+input.toString());

		List<String> gpsLineList=null;  // List one sequence of data: count,time,vehicleIdsInThisArea
        int sizeofGPSLine=input.size();
        
        //Object[] 
        List<Object>		countInput=input.getValues();
        String  [] countBoltInput =countInput.toString().split(TupleInfo.getDelimiter());
        for(int i =0;i<countBoltInput.length;i++)
        FieldListenerSpout.writeToFile("/home/ghchen/output","CountBolt countBoltInput["+i+"]:"+countBoltInput[i]);
        
        String districtID =countBoltInput[7].replace("]", "");
		double lan= Double.parseDouble(countBoltInput[5]);
		double lon= Double.parseDouble(countBoltInput[6]);
        
//		String districtID =  input.getValues().get(7).toString();
//		double lan= (Double)input.getValues().get(5);
//		double lon= (Double)input.getValues().get(6);
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//Date dateTime = sdf.parse("2013-01-15 22:11:02");
		Date dateTime = null;
		long  interval=0;
		double dist=Math.sqrt(Math.pow(lan-lanLast,2)+Math.pow(lon-lonLast,2));	
		try {
			dateTime = sdf.parse( countBoltInput[1].replace("[", ""));//.toString() );
			FieldListenerSpout.writeToFile("/home/ghchen/output","dataTime:"+dateTime);
			 // convert dateTime form string to class Date	
			interval=(dateTime.getTime()-dateTimeLast.getTime())/1000;
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("CountBolt Error: can't assign value of Index [1] in input tuple !");
		} 
		
		if(!districts.containsKey(districtID)){	
			lanLast=lan;
			lonLast=lon;
			dateTimeLast=dateTime;	
		}
	    
		/** If the word dosn't exist in the map we will create
		 * this, if not We will creat a new thread and  add 1 */		
		if(dist>DIST0 && interval>INTERVAL0){
			if(districts.containsKey(districtID)){	
				cnt =Integer.parseInt(districts.get(districtID).get(1)) + 1;
				vehicleIdsInThisArea.add((String) input.getValues().get(1));

				gpsLineList.add(districtID);
				gpsLineList.add(cnt.toString());
				gpsLineList.add(input.getValues().get(1).toString());	// get Time stamp from input	
				gpsLineList.addAll(vehicleIdsInThisArea);  

				districts.put(districtID, gpsLineList);	

			}
			else{
				cnt=1;
				vehicleIdsInThisArea.add((String) input.getValues().get(1));
				
				gpsLineList.add(districtID);
				gpsLineList.add(cnt.toString());
				gpsLineList.add(input.getValues().get(1).toString());	// get Time stamp from input	
				gpsLineList.addAll(vehicleIdsInThisArea);  				
				
				districts.put(districtID, gpsLineList);
			}
			
			FieldListenerSpout.writeToFile("/home/ghchen/output","CountBolt districts:"+districts.toString());
		
    		lanLast = lan;
			lonLast = lon;
			try {
				dateTimeLast=sdf.parse( (String)input.getValues().get(1));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		_collector.emit(new Values(districts));

		}

		
		Date nowtime=new Date();
		int  timeMinute= nowtime.getMinutes();	
		
		/* Every ten minute, we reset the list to null;	 * */		
		if(0==(timeMinute%10)){   //every 10 minutes
	FieldListenerSpout.writeToFile("/home/ghchen/output","CountBolt vehicleIdsInThisArea:"+vehicleIdsInThisArea.toString());
	
			cnt=0;
			vehicleIdsInThisArea=null;
		}			
		_collector.ack(input);
	}

	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
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
