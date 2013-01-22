/**
 * realODMatrix main.java.realODMatrix.spout TupleInfo2.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-14 上午11:27:05
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.spout;

import java.util.List;

import backtype.storm.tuple.Fields;
import main.java.realODMatrix.spout.JavaReadXml;

/**
 * realODMatrix main.java.realODMatrix.spout TupleInfo2.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-14 上午11:27:05
 * email: gh.chen@siat.ac.cn
 *
 */
public class TupleInfo2 {
	
	List<String>  gpsLineSplit=null;
	
	JavaReadXml  jrx = new JavaReadXml("tupleInfo.xml");
	
	
	public TupleInfo2(String[] input) throws Exception{
		
		for (int i=0;i< input.length;i++)
		{
			gpsLineSplit.add(input[i])	;		
		}
		
//		gpsLineSplit[0]          //viechleID
//		gpsLineSplit[1]=input[1]; //dateTime
//		gpsLineSplit[2]=Integer.parseInt(input[2]); //occupied
//		gpsLineSplit[3]=Integer.parseInt(input[3]); //speed
//		gpsLineSplit[4]=Integer.parseInt(input[4]);	//bearing		 
//		gpsLineSplit[5]=Double.parseDouble(input[5]); //latitude
//		gpsLineSplit[6]=Double.parseDouble(input[6]);  // longitude		

	}
	
	public int getTupleLength() {	
		
		return jrx.getTupleLength();
	}
	
	
	public Fields getFieldList() {
		
		Fields fld= new Fields(jrx.getFieldList());
			
		return fld ;
		}




	public String getDelimiter() {
		// TODO Auto-generated method stub
        return jrx.getDelimiter();
	
	}	
	
	public void main() throws Exception{
		String[] strings = null;
		strings[0]="0";
		strings[1]="1";
		strings[2]="2";
		strings[3]="3";
		strings[4]="4";
		strings[5]="5";
		strings[6]="6";
		
		TupleInfo2 t2= new TupleInfo2(strings);
		System.out.println(t2.getDelimiter());
		System.out.println(t2.getFieldList());
		System.out.println(t2.getTupleLength());	
		
			
		
	}
	
	
}


