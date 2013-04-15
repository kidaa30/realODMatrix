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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.List;

import java.util.Timer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.map.StaticBucketMap;
import org.apache.hadoop.conf.Configuration;

//import storm.realTraffic.bolt.MySqlClass;
//import storm.realTraffic.bolt.SpeedCalculatorBolt2;
//import storm.realTraffic.bolt.SpeedCalculatorBolt2.Road;


/**
 * realODMatrix realODMatrix.bolt CountBolt.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 2:45:05
 * email: gh.chen@siat.ac.cn
 *
 */
public class CountBolt2 implements IRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	double lanLast;   // last location of the vehicle
	double lonLast;
	Date dateTimeLast=null;
	int INTERVAL0 = 120/2; // We set time windows between two points 120 seconds;
	double DIST0=0.008993/2;  //  On the Earth, 1 Degree =111.2 km 
	                        //Distance between two points 1km, shoule be 1/111.2 =0.008993 Degree;
	
	private OutputCollector _collector;	
	Integer taskId;
	String taskName;
	//Map<String, List<String> > districts; //DistrictID, vehicleIdsInThisArea
	public  LinkedList<District>  districts = new  LinkedList<District>();
	//static public List<String> vehicleIdsInThisArea=new ArrayList<String>(); 
	Integer cnt;
	public Timer timer;
	static Configuration conf=null ;
	static HBaseHelper helper=null;
	public   Map<String,String> lastDrictMap=new HashMap<String, String>();  //DistrictID, vID
	public   Map<String,Integer> countMap=new ConcurrentHashMap<String, Integer>();
	//public Map<String,Integer> matrix=new HashMap<String, Integer>();
	MySqlClass mysql=null; 
	//public Map<String, Integer> countmap2=new ConcurrentHashMap<String, Integer>();

	
	public class District 
	{
		public String districtId;
		public int count;//计算次数，是车牌号的个数码
		public Date dateTime; //该小区统计的车辆出现时间
		public HashMap<String,Date> viechleIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		public HashMap<String,String> vieLngLatIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
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
    /**
     * Map <VehicleID, DistrictID>
     */
    public  Boolean isDisExits(Map<String, String> lastDistrictMap,  String VehicleID){
    	for(Entry<String, String> e: lastDistrictMap.entrySet()){
			if(e.getKey().equals(VehicleID)){
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
		double lan = Double.parseDouble(input.getValues().get(5).toString());//
		double lon = Double.parseDouble(input.getValues().get(6).toString()); //
		String viechId = input.getValues().get(0).toString();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateTime = null;
		try {
			dateTime = sdf.parse(input.getValues().get(1).toString());
			
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		
		if (!countMap.containsKey(districtID)) { //小区的第一辆车
			Integer count=new Integer(1);
			countMap.put(districtID, count);
			if(lastDrictMap.containsKey(viechId)){
				String lastDistrID=lastDrictMap.get(viechId);
				if(!lastDistrID.equals(districtID)){							
					Integer lastCount=countMap.get(lastDistrID)	;
					lastCount=lastCount-1;
					countMap.put(lastDistrID, lastCount);					
				}				
			}
			lastDrictMap.put(viechId, districtID);		
		}else{
			Integer currentCnt=countMap.get(districtID);
			
			if(!lastDrictMap.containsKey(viechId)){  //该小区存在，但是该车第一次出现				
				currentCnt=currentCnt+1;
				countMap.put(districtID, currentCnt);
			}else{
				String lastDistrID=lastDrictMap.get(viechId);				
				if(!lastDistrID.equals(districtID)){
					Integer lastCount=countMap.get(lastDistrID)	;
					lastCount=lastCount-1;
					countMap.put(lastDistrID, lastCount);					
					currentCnt=currentCnt+1;
					countMap.put(districtID, currentCnt);					
				}				
			}
			
			lastDrictMap.put(viechId, districtID);
		}
			if(mysql==null) mysql=new MySqlClass("172.20.36.247","3306","realOD", "ghchen", "ghchen");
						
/*			Job= new TimerTask() {		
				@Override
				public void run() {
					 synchronized(this) { 
							mysql.query("delete from realOD.count");
							CountBolt2.writeToMysql(mysql, countMap);

				}
				}
			};
			timer=new Timer(true);
			timer.schedule(Job,5000, 30000);  //every 10 seconds.
*/		
		Date nowDate=new Date();
		SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		SimpleDateFormat sdf3= new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf4= new SimpleDateFormat("yyyy-MM-dd-HH");
		int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		
		String cur_dir=System.getProperty("user.dir");
		 cur_dir=cur_dir+"/"+sdf3.format(nowDate);
		 newFolder(cur_dir);	 


		
		/*else*/ if(second%20==0){
			String nowTime=sdf2.format(nowDate);
			cur_dir=cur_dir+"/"+"count";
			 newFolder(cur_dir);
			 cur_dir=cur_dir+"/"+nowTime;
			 //if(mysql==null) mysql=new MySqlClass("172.20.36.247","3306","realOD", "ghchen", "ghchen");
			 try {
			    mysql.query("delete from realOD.count");
				this.writeToMysql(mysql, countMap);				
				 
					
				CountBolt2.writeToFile(cur_dir,countMap);	

				System.out.println("\n\n\n---------------SIZE of lastDrictMap = "+lastDrictMap.size()+"-------\n\n\n");
				System.out.println("\n\n\n---------------SIZE of  countMap    = "+countMap.size()+"-------\n\n\n");
				Thread.sleep(1000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 

			 
		}
		
	
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
    			if(d.count>0){
    				br.write("\n"+d.districtId+","+d.count+"#");
    				br.flush();
    				for(Map.Entry<String,Date> entry : d.viechleIDList.entrySet()){   //
    					String lonLanString=d.vieLngLatIDList.get(entry.getKey()); 
    					//if(entry.getKey()!=null && entry.getValue()!=null && lonLanString!=null)
    					br.write(entry.getKey()+","+sdf.format(entry.getValue()) +","+lonLanString+";");
    					br.flush();
    					System.out.println(entry.getKey()+","+entry.getValue()+","+lonLanString+";");
    				}
    				//br.flush();
//    				br.write("\r\n");
//    				br.flush();

    				//System.out.println("\n");
    			}         

    			
    		}
    		br.close();		      
    		// districts.clear();				
    	} catch (IOException e1) {
    		// TODO Auto-generated catch block
    		e1.printStackTrace();
    	}		
    }
    private static void writeToFile(String fileName,Map<String, Integer>  content) throws IOException{
    	String[] name=fileName.split("/");
    	String tmp=null;
    	if(name[name.length-1].length()>13) {tmp=fileName.substring(0, fileName.length()-6);
    	}else{
    		tmp=fileName;
    	}
    	BufferedWriter br = new BufferedWriter(new FileWriter(tmp,true));
    	for(Entry<String, Integer> entry : content.entrySet()){   //
			br.write(name[name.length-1]+","+entry.getKey()+","+entry.getValue()+"\r\n");
			br.flush();
			System.out.println(entry.getKey()+","+entry.getValue());
		}
    	
    }
    
    
	
    private static void writeToFile(String fileName, String text) throws IOException{
    	BufferedWriter br = new BufferedWriter(new FileWriter(fileName,true));
    	br.write(text);
    	br.flush();
    }
    	
    public synchronized void writeToMysql(MySqlClass mysql,Map<String,Integer> countMap){
       	SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String nowtime=sdf.format(new Date());
    	for(Entry<String, Integer> entry : countMap.entrySet()){
    		int rs = mysql.query("insert into realOD.count(time,districtID,count) values('"+nowtime
    				+"','"+entry.getKey()+"',"+entry.getValue()+" );");   
    		//("insert into realOD.count(time,districtID,count) values('2013-04-09 12:00:59',10101,99 );")
    		if(rs!=0) System.out.println("Insert into Mysql success :   "+entry.getKey()+"',"+entry.getValue());
    	} 

    }
    
  
	
    public static void newFolder(String folderPath) { 
    	try { 
    		String filePath = folderPath.toString(); 
    		//filePath = filePath.toString(); 
    		java.io.File myFilePath = new java.io.File(filePath); 
    		if (!myFilePath.exists()) { 
    			myFilePath.mkdir(); 
    		} 
    	} 
    	catch (Exception e) { 
    		System.out.println("Eorror: Can't create new folder!"); 
    		e.printStackTrace(); 
    	} 
    }

}
