/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 下午1:57:46
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

//port backtype.storm.daemon.nimbus.newly_added_slots;

import com.vividsolutions.jts.index.bintree.Interval;

/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 下午1:57:46
 * email: gh.chen@siat.ac.cn
 *
 */
public class test {

	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
		Date dateTime;
		try {
			dateTime = sdf.parse("2012-08-15-22-11-02");
			
			String dateString = sdf.format(dateTime);
			int  minute= dateTime.getMinutes();
			
		    String nowtime=sdf.format(new Date());
		    
		    
		    System.out.println("dateTime minutes="+minute);
			System.out.println("dateTime="+dateTime);
			System.out.println("nowString="+dateString);
			//System.out.println("nowTime="+System.currentTimeMillis());
			System.out.println("nowTime="+nowtime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Date nowTime= new Date();
		//Date nowTime =new Date(System.currentTimeMillis());//FormatDateTime.toLongDateString(now);
		//long Interval = nowTime.getTime() - dateTime.getTime();
	
		
		
		//System.out.println("nowTime="+nowTime);
		//System.out.println(Interval);

	}

}
