/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 閿熸枻鎷烽敓鏂ゆ嫹2:45:05
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimerTask;


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
import main.java.realODMatrix.struct.GPSRcrd;

import java.util.Timer;


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
	//Map<String, List<String> > districts; //DistrictID, vehicleIdsInThisArea
	public  LinkedList<District>  districts = new  LinkedList<District>();
	List<String> vehicleIdsInThisArea=new ArrayList<String>(); 
	Integer cnt;
	Timer timer;
	
	public class District 
	{
		public String districtId;
		public int count;//璁＄畻娆℃暟锛屾槸杞︾墝鍙风殑涓暟鐮�
		public Date dateTime; //搴旇鏄灏忓尯缁熻鐨勬椂鍊欑殑璧峰鏃堕棿
		public HashMap<String,Date> viechleIDList; //瀛樻斁杞﹁締Id鐨勯泦鍚�涔熻鎶婃椂闂村瓨鑰咃紝浠ュ姣忎竴杈嗚溅杩涜璁＄畻鏃堕棿璺濈
		public HashMap<String,String> vieLngLatIDList; //瀛樻斁杞﹁締Id鐨勯泦鍚�涔熻鎶婃椂闂村瓨鑰咃紝浠ュ姣忎竴杈嗚溅杩涜璁＄畻鏃堕棿璺濈
	}
	
	public  District  getDistrictById(String districtId){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
				return d;
			}
		}
		return null;
	}
	
	public  String getlngLatByViecheId(String districtId,String viechId){
		for(District d : districts){
			if(d.districtId.equals(districtId)){
				return  d.vieLngLatIDList.get(viechId);
			}
		}
		return null;
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
		return null;
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
		// TODO Auto-generated method stub
		this.taskName = context.getThisComponentId();
		this.taskId = context.getThisTaskId();
		this._collector = collector;		
	}

	
	@SuppressWarnings("null")
	@Override
	public void execute(Tuple input) {
		
		String districtID = input.getValues().get(7).toString();
		double lan = Double.parseDouble(input.getValues().get(5).toString());//绾害
		double lon = Double.parseDouble(input.getValues().get(6).toString()); //缁忓害
		String viechId = input.getValues().get(0).toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateTime = null;
		try {
			dateTime = sdf.parse(input.getValues().get(1).toString());
			
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		if (!isDisExits(districts, districtID)) {
			 //娌℃湁姝ゅ皬鍖猴紝鍒欐柊寤轰竴涓皬鍖猴紝骞跺瓨璧锋潵			
			//System.out.println("districtID:"+districtID+"dateTime:"+dateTime+"viechId"+viechId);
			District district = new District();
			district.viechleIDList = new HashMap<String,Date>() ; //瀛樻斁杞﹁締Id鐨勯泦鍚�涔熻鎶婃椂闂村瓨鑰咃紝浠ュ姣忎竴杈嗚溅杩涜璁＄畻鏃堕棿璺濈
			district.vieLngLatIDList= new HashMap<String,String>() ;
			
			district.districtId = districtID;
			district.count = 1;
			district.dateTime = dateTime;
			district.viechleIDList.put(viechId, dateTime);
			district.vieLngLatIDList.put(viechId, lon+"_"+lan);
			
			districts.add(district);  //娣诲姞灏忓尯
			return ;
			  
		}else{   //濡傛灉宸茬粡鏈夎灏忓尯
			District district=getDistrictById(districtID);
			if(!district.viechleIDList.containsKey(viechId)){  //浣嗘槸濡傛灉杞﹁締ID鏄涓�杩涘叆璇ュ尯鍩燂紝鏂板缓涓�釜杞﹁締ID锛屽苟淇濆瓨锛�
				district.count++;
				district.dateTime = dateTime;
				district.viechleIDList.put(viechId, dateTime);
				district.vieLngLatIDList.put(viechId, lon+"_"+lan);				
			}else{ //鍚﹀垯锛岃繖杈嗚溅鏄娆″嚭鐜板湪璇ュ尯鍩燂紝鍒欏垽鏂繖涓溅杈咺D 鍜屼笂涓�鍑虹幇鐨勬椂闂撮棿闅斿拰璺濈
				String lngLat = getlngLatByViecheId(districtID,viechId);
				String[]  s = lngLat.split("_");
				lonLast = Double.parseDouble(s[0]);
				lanLast = Double.parseDouble(s[1]);
				
				long interval = 0;
				dateTimeLast = getDateByViecheId(districtID, viechId);
				interval = (dateTime.getTime() - dateTimeLast.getTime()) / 1000;
				double dist = Math.sqrt(Math.pow(lan - lanLast, 2) + Math.pow(lon - lonLast, 2));
				
				if (dist > DIST0 && interval > INTERVAL0) {					
					
					district.count ++;
					district.dateTime = dateTime;
					district.viechleIDList.put(viechId, dateTime);
					district.vieLngLatIDList.put(viechId, lon+"_"+lan);	
				}else{
					return;					
				}
			}
		}
				

		Date nowDate=new Date();
		SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		if( (min%5) ==0 && (second==0) ){
			String nowTime=sdf2.format(nowDate);
			CountBolt.writeToFile("vehicleList-"+nowTime,districts);	
		}
		
//		timer=new Timer(true);
//		TimerTask Job= new TimerTask() {		
//			@Override
//			public void run() {
//				SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
//				String nowtime=sdf.format(new Date());
//				CountBolt.writeToFile("vehicleList-"+nowtime,districts);
//			}
//		};
//		timer.schedule(Job,0, 60*1000);  //every 600 seconds.

		
		_collector.ack(input);
	
	}

	
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+taskName+"-"+taskId+"] --");
//		for(Map.Entry<GPSRcrd, Integer> entry : gpsMatch.entrySet()){
//		System.out.println(entry.getKey()+": "+entry.getValue());
//		}
	
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
	
    static class Job extends java.util.TimerTask{   
        @Override  
        public void run() {   
            // TODO Auto-generated method stub  
         
        }  
    } 
    
	public static void writeToFile(String fileName, LinkedList<District> districts){
		try {
              BufferedWriter br = new BufferedWriter(new FileWriter(fileName,true));
     		  SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				String nowtime=sdf.format(new Date());
     		  // ddDistrict=districts;
              for(District d:districts){
//            	  br.write(d.districtId+","+d.count+"#"+d.viechleIDList.values()+";"+
//                    d.vieLngLatIDList.values()+"\n"); 
            	  br.write(d.districtId+","+d.count+"#");
          		for(Map.Entry<String,Date> entry : d.viechleIDList.entrySet()){   //
          			String lonLanString=d.vieLngLatIDList.get(entry.getKey()); 
          			//if(entry.getKey()!=null && entry.getValue()!=null && lonLanString!=null)
          			br.write(entry.getKey()+","+sdf.format(entry.getValue()) +","+lonLanString+";");
         			//System.out.println(entry.getKey()+","+entry.getValue()+","+lonLanString+";");
          			}
          		br.write("\r\n");

          		//System.out.println("\n");
              }         
           
              /*for(District d : districts){
               	  br.write(d.districtId + ","+ d.count + "#");
            	  HashMap<String ,String> viechIds = d.vieLngLatIDList;
            	  Set<String> set = viechIds.keySet();
            	  Iterator<String> iterator = set.iterator();
            	  while(iterator.hasNext()){
            		  String id = iterator.next();
            		  br.write(id+"    ");
            	  }
              }*/
		      br.flush();
		      br.close();		      
        	  districts.clear();				
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}


}
