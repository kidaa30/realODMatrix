/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-29 下午4:30:55
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import main.java.realODMatrix.bolt.CountBolt.District;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-29 下午4:30:55
 * email: gh.chen@siat.ac.cn
 *
 */
public class ToHbase {

	public static void main(String[] args) throws IOException {

		String path="/home/ghchen/vehicleList-2013-03-12-14-10-00";
		//LinkedList<District>  dlist=  read2list(path);
		SimpleDateFormat   sDateFormat   =   new   SimpleDateFormat("yyyy-MM-dd hh:mm:ss");     
		String   date   =   sDateFormat.format(new   java.util.Date());  

		//writeToHbase("realOD2Hbase", date, dlist);

		}

	/*public static class District 
	{
		public String districtId;
		public int count;//计算次数，是车牌号的个数码
		public Date dateTime; //该小区统计的车辆出现时间
		public HashMap<String,String> viechleIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		public HashMap<String,String> vieLngLatIDList; //存放车辆Id的集合,也要把时间存者，以对每一辆车进行计算时间距离
		District(String districtId,Date dateTime,HashMap<String,String> viechleIDList,HashMap<String,String> vieLngLatIDList){
			this.districtId=districtId;
			this.dateTime=dateTime;
			this.viechleIDList=viechleIDList;
			this.vieLngLatIDList=vieLngLatIDList;
		} 
	}*/


	/*public static  LinkedList<District> read2list(String path) throws IOException{
		HashMap<String,Date> viechleIDList=new HashMap<String, Date>();
		HashMap<String,String> vieLngLatIDList=new HashMap<String, String>();
		String districtId=new String();
		Date dateTime=new Date();
		District d=new CountBolt.District(districtId, dateTime, viechleIDList, vieLngLatIDList);
		LinkedList<District> dList= new LinkedList<District>();
		String line=null;
		//BufferedReader fileReader = new BufferedReader(new FileReader(new File(path))); 
          //while ((line = fileReader.readLine()) != null)
		line="335,194#粤BK1Y40,2013-03-12 14:05:38,113.971169_22.586567;粤BL0C17,2013-03-12 14:09:12,113.949715_22.583099;";
          {             	   
             	 String[] tmp1 =line.split("#");
             	 String[] tmp2 =tmp1[0].split(",");
             	 d.districtId=tmp2[0];
             	 d.count=Integer.parseInt(tmp2[1]);
             	 String[] tmp3=tmp1[1].split(";");
             	 for(int i=0;i<tmp3.length;i++){             		
             		String[]  tmp4=tmp3[i].split(",");
             		 if(tmp4.length>=3){
             		d.viechleIDList.put(tmp4[0], tmp4[1]);
             		d.vieLngLatIDList.put(tmp4[0], tmp4[2]);
             		 }
             	 }
             	dList.add(d);
             } 
		 
		  return dList;	
		
		
	}*/




/*	public static void writeToHbase(String tableName, String Time,LinkedList<District> districts) throws IOException{

				Configuration conf = HBaseConfiguration.create() ;
				HBaseHelper helper= HBaseHelper.getHelper(conf);
				if (!helper.existsTable(tableName)) { 

					helper.createTable(tableName, "cntFamily","RecordFamily"); 
				} 

				else System.out.println("\n\n Hbasetest: Table named \"testTable\" exist ! " );

				HTable table = new HTable(conf, tableName);                    // co GetExample-2-NewTable Instantiate a new table reference.  


				for(District d:districts){
					String row=Time+"_"+d.districtId;
					Integer count=d.count;
					String gpsRcd=new String();	
					for(Entry<String, Date> entry : d.viechleIDList.entrySet()){   //
						String lonLanString=d.vieLngLatIDList.get(entry.getKey()); 

						gpsRcd=gpsRcd+entry.getKey()+","+entry.getValue()+","+lonLanString+";";						
					}
					helper.put(tableName, row, "cntFamily", "count",count.toString() );
					helper.put(tableName, row, "RecordFamily", "vehicleRecord",gpsRcd ); 

					Get get = new Get(Bytes.toBytes(row));                             // co GetExample-3-NewGet Create get with specific row.  
					get.addColumn(Bytes.toBytes("cntFamily"), Bytes.toBytes("count")); // co GetExample-4-AddCol Add a column to the get.  
					get.addColumn(Bytes.toBytes("RecordFamily"), Bytes.toBytes("vehicleRecord"));
					Result result = table.get(get);                                    // co GetExample-5-DoGet Retrieve row with selected columns from HBase. 
					byte[] val = result.getValue(Bytes.toBytes("cntFamily"), Bytes.toBytes("count")); // co GetExample-6-GetValue Get a specific value for the given column.  
					System.out.print("Count: " + Bytes.toString(val));               // co GetExample-7-Print 
					val = result.getValue(Bytes.toBytes("RecordFamily"), Bytes.toBytes("vehicleRecord")); 
					System.out.println("Vehicle List: " + Bytes.toString(val));

				}


			}*/




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

	public static void writeToHbase(HBaseHelper helper,String tableName, String Time,LinkedList<District> districts) throws IOException {
		//Configuration conf = HBaseConfiguration.create() ;
		//HBaseHelper helper= HBaseHelper.getHelper(conf);
		 SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (!helper.existsTable(tableName)) { 

			helper.createTable(tableName, "cntFamily","RecordFamily"); 
		} 

		else System.out.println("\n\n Hbasetest: Table named \"testTable\" exist ! " );



		for(District d:districts){
			String row=Time+"_"+d.districtId;
			Integer count=d.count;
			if(count>=0){
			String gpsRcd=new String();	
			for(Entry<String, Date> entry : d.viechleIDList.entrySet()){   //
				String lonLanString=d.vieLngLatIDList.get(entry.getKey()); 

				gpsRcd=gpsRcd+entry.getKey()+","+sdf.format(entry.getValue())+","+lonLanString+";";						
			}
			helper.put(tableName, row, "cntFamily", "count",count.toString() );
			helper.put(tableName, row, "RecordFamily", "vehicleRecord",gpsRcd ); 
			}
/*			HTable table = new HTable(conf, tableName);                    // co GetExample-2-NewTable Instantiate a new table reference.  
			Get get = new Get(Bytes.toBytes(row));                             // co GetExample-3-NewGet Create get with specific row.  
			get.addColumn(Bytes.toBytes("cntFamily"), Bytes.toBytes("count")); // co GetExample-4-AddCol Add a column to the get.  
			get.addColumn(Bytes.toBytes("RecordFamily"), Bytes.toBytes("vehicleRecord"));
			Result result = table.get(get);                                    // co GetExample-5-DoGet Retrieve row with selected columns from HBase. 
			byte[] val = result.getValue(Bytes.toBytes("cntFamily"), Bytes.toBytes("count")); // co GetExample-6-GetValue Get a specific value for the given column.  
			System.out.print("Count: " + Bytes.toString(val));               // co GetExample-7-Print 
			val = result.getValue(Bytes.toBytes("RecordFamily"), Bytes.toBytes("vehicleRecord")); 
			System.out.println("Vehicle List: " + Bytes.toString(val));*/

		}


		
	}





}