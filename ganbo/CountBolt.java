/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:45:05
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.command.list;
import backtype.storm.daemon.nimbus.newly_added_slots;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.List;

import org.joda.time.DateTime;

import clojure.string__init;

import clojure.core.class;

import main.java.realODMatrix.spout.FieldListenerSpout;
import main.java.realODMatrix.spout.TupleInfo;

/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 * 
 * Copyright 2013 Xdata@SIAT Created:2013-1-8 2:45:05 email: gh.chen@siat.ac.cn
 * 
 */
public class CountBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private  Double lanLast; // last location of the vehicle
	private Double lonLast;
	private Date dateTimeLast;
	private int INTERVAL0 = 120; // We set time windows between two points 120 seconds;
	private Double DIST0 = 0.008993; // On the Earth, 1 Degree =111.2 km Distance between two points 1km, shoule be	1/111.2 =0.008993 Degree;
	public  Integer taskId;
	public   String taskName;
	public  LinkedList<District>  districts = new  LinkedList<District>();
	private OutputCollector _collector;
	
	public class District 
	{
		public String districtId;
		public int count;//计算次数，是车牌号的个数码
		public Date dateTime; //应该是该小区统计的时候的起始时间
		public HashMap<String,Date> viechleIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		public HashMap<String,String> vieLngLatIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
	}
	
	public  District  getDistrictById(String districtId){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
				return d;
			}
		}
	}
	
	public  String getlngLatByViecheId(String districtId,String viechId){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
				return  d.vieLngLatIDList.get(viechId);
			}
		}
	}
	
	public  void setlngLatByViecheId(String districtId,String viechId,String lngLat){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
			   d.vieLngLatIDList.put(viechId, lngLat);
			}
		}
	}
	
	public  Date getDateByViecheId(String districtId,String viechId){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
				return  d.viechleIDList.get(viechId);
			}
		}
	}
	
	public  void setDateByViecheId(String districtId,String viechId,Date dateTime){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
			   d.viechleIDList.put(viechId, dateTime);
			}
		}
	}
	
	
    public  Boolean isDisExits(List<District>  districts,  String districtId){
    	for(District d : districts){
			if(d.districtId.equals(districtId)){
				return true;
			}
		}
    	return false;
    }
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
		this._collector = collector;
	}
    /*
     一次处理一行记录
     **/
	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {
		String districtID = input.getValues().get(7).toString();
		double lan = Double.parseDouble(input.getValues().get(5).toString());//纬度
		double lon = Double.parseDouble(input.getValues().get(6).toString()); //经度
		String viechId = input.getValues().get(0).toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateTime = null;
		try {
			dateTime = sdf.parse(input.getValues().get(1).toString());
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		if (!isDisExits(districts, districtID)) {
				//没有此小区，则新建一个小区，并存起来
				District district = new District();
				district.districtId = districtID;
				district.count = 1;
				district.dateTime = dateTime;
				district.viechleIDList.put(viechId, dateTime);
				district.vieLngLatIDList.put(viechId, lon+"_"+lan);
				districts.add(district);
				return ;
			}
		//如果已经有该小区
		long interval = 0;
		String lngLat = getlngLatByViecheId(districtID,viechId);
		String[] s = lngLat.split("_");
		lonLast = Double.parseDouble(s[0]);
		lanLast = Double.parseDouble(s[1]);
		dateTimeLast = getDateByViecheId(districtID, viechId);
		interval = (dateTime.getTime() - dateTimeLast.getTime()) / 1000;
		double dist = Math.sqrt(Math.pow(lan - lanLast, 2) + Math.pow(lon - lonLast, 2));

		/**
		 * If the word dosn't exist in the map we will create this, if not We
		 * will create a new thread and add 1
		 */
		District d = getDistrictById(districtID);
		if (dist > DIST0 && interval > INTERVAL0) {
				
				d.count ++;
//			FieldListenerSpout.writeToFile("/home/ghchen/vehicleIdsInThisArea",
//					"vehicleIdsInThisArea:" + vehicleIdsInThisArea.toString());
//			FieldListenerSpout.writeToFile("/home/ghchen/districts",
//					"CountBolt districts:" + districts.toString());
			_collector.emit(new Values(districts));

		}
		
		long t1 = dateTime.getTime();
		long t2 = d.dateTime.getTime();
		long diff = (t1 - t2)/(1000*600);
		if(diff >= 1 ){
			//写文件
			FieldListenerSpout.writeToFile("/home/ghchen/vehicleIdsInThisArea",districts);
			d.count = 0;
			//_collector.ack(input);//这句话写在哪里？
		}
		_collector.ack(input);//这句话写在哪里？
	}

	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + taskName + "-" + taskId
				+ "] --");
		for (Map.Entry<String, List<String>> entry : districts.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
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
