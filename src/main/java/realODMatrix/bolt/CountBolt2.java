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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import main.java.realODMatrix.bolt.ToHbase;

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
	Timer timer;
	static Configuration conf=null ;
	static HBaseHelper helper=null;
	public Map<String,String> lastDrictMap=new HashMap<String, String>();  //DistrictID, vID
	public Map<String,Integer> countMap=new HashMap<String, Integer>();
	public Map<String,Integer> matrix=new HashMap<String, Integer>();
	
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
			if(!lastDrictMap.containsKey(viechId)){ //该车第一次出现
				lastDrictMap.put(viechId, districtID);
			}else{
				String lastDistrID=lastDrictMap.get(viechId);
				if(lastDistrID.equals(districtID)){
					return;
				}else{
					Integer lastCount=countMap.get(lastDistrID)	;
					lastCount=lastCount-1;
					countMap.put(lastDistrID, lastCount);

					String key=lastDistrID+"-"+districtID;
					if(!matrix.containsKey(key)){
						Integer mCount=1;
						matrix.put(key, mCount);					
					}else{
						Integer mCount=matrix.get(key);
						mCount=mCount+1;
						matrix.put(key, mCount);	
					}
				}
			}			
		}else{
			if(!lastDrictMap.containsKey(viechId)){  //该小区存在，但是该车第一次出现
				lastDrictMap.put(viechId, districtID);

				Integer currentCnt=countMap.get(districtID);
				currentCnt=currentCnt+1;
				countMap.put(districtID, currentCnt);

			}else{
				String lastDistrID=lastDrictMap.get(viechId);
				if(lastDistrID.equals(districtID)){
					return;
				}else{
					Integer lastCount=countMap.get(lastDistrID)	;
					lastCount=lastCount-1;
					countMap.put(lastDistrID, lastCount);

					Integer currentCnt=countMap.get(districtID);
					currentCnt=currentCnt+1;
					countMap.put(districtID, currentCnt);
					lastDrictMap.put(viechId, districtID);

					String key=lastDistrID+"-"+districtID;
					if(!matrix.containsKey(key)){
						Integer mCount=1;
						matrix.put(key, mCount);					
					}else{
						Integer mCount=matrix.get(key);
						mCount=mCount+1;
						matrix.put(key, mCount);	
					}
				}
			}
		
		Date nowDate=new Date();
		SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		SimpleDateFormat sdf3= new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf4= new SimpleDateFormat("yyyy-MM-dd-HH");
		int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		
		String cur_dir=System.getProperty("user.dir");
		 cur_dir=cur_dir+"/"+sdf3.format(nowDate);
		 newFolder(cur_dir);	

		 
		if(min==0 && second==0){
			 cur_dir=cur_dir+"/"+"ODvector";
			 newFolder(cur_dir);
			 String matrixDir=cur_dir+"/"+sdf4.format(nowDate);
			try {
				CountBolt2.writeToFile(matrixDir,matrix);
				matrix.clear();
				Thread.sleep(1000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
				 
		/* TimerTask writeCnt=new TimerTask() {			
			@Override
			public void run() {					
				CountBolt2.writeToFile(cur_dir,countMap);
				countMap.clear();
			}
		};
		
		 TimerTask writeMatrix=new TimerTask() {			
			@Override
			public void run() {					
				CountBolt2.writeToFile(cur_dir,countMap);
				countMap.clear();
			}
		};
		
		
		 Timer timer = new Timer(); 
		  timer.schedule(writeCnt, 10000, 10000);  
		  timer.schedule(writeMatrix, 1800*1000, 3600*1000);  */
		
		else if(second%10==0){
			String nowTime=sdf2.format(nowDate);
			cur_dir=cur_dir+"/"+"count";
			 newFolder(cur_dir);
			 cur_dir=cur_dir+"/"+nowTime;
			 
			 try {
			//	CountBolt2.writeToFile(cur_dir,"##"+nowTime+"##"); 
				CountBolt2.writeToFile(cur_dir,countMap);
				//countMap.clear();

				Thread.sleep(1000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/*if( (min%2) ==0 && (second==0) ){
			String nowTime=sdf2.format(nowDate);

			    
			LinkedList<District> d=new  LinkedList<District> (districts);
			districts.clear();
			 
			 String cur_dir=System.getProperty("user.dir");
			 cur_dir=cur_dir+"/"+sdf3.format(nowDate);
			 newFolder(cur_dir);
			 
			 cur_dir=cur_dir+"/"+"vehicleList-"+nowTime;
			 CountBolt2.writeToFile(cur_dir,d);
			 d.clear();
			 try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
/*		try {
			
			if(helper==null){
					conf = HBaseConfiguration.create() ;
					helper= HBaseHelper.getHelper(conf);
					//ToHbase.writeToHbase(helper,"realOD2Hbase", nowTime, d);
			 }else{
			   ToHbase.writeToHbase(helper,"realOD2Hbase", nowTime, d);
			 }
			Thread.sleep(1000);
			
			d.clear();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
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
