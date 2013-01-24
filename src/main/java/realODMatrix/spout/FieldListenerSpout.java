package main.java.realODMatrix.spout;

//import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import main.java.realODMatrix.spout.TupleInfo;
import main.java.realODMatrix.struct.GPSRcrd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import main.java.realODMatrix.bolt.CountBolt.District;


public class FieldListenerSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
    private BufferedReader fileReader;
    //private TopologyContext context;
    //private String file="/home/ghchen/2013-01-05.1/2013-01-05--11_05_48.txt";
    private TupleInfo tupleInfo=new TupleInfo();
    
    //Fields fields;
    
    static Socket sock=null;
    
    @Override
    public void close() {
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
		{
		    _collector = collector;
		    
		    try {
		    	if(sock==null){
		    		sock=new Socket("172.20.14.204",15025);
		    	}
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}   

		    
/*		 String file=new String();
			 if(file.equals(""))
			 {
				file="/home/ghchen/GPS_2011_09_27.txt";
			 }
			 
		try 
			{	

			  this.fileReader = new BufferedReader(new FileReader(new File(file))); 
			} 
		catch (FileNotFoundException e) 
			{
				throw new RuntimeException ("error reading file ["+file+"]");
			}*/
		    
		}
//	} catch (Exception e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}

    @SuppressWarnings("unused")
	@Override
    public void nextTuple() {    	
   
        Utils.sleep(2000);
       // RandomAccessFile access = null; 
        /*String line = null;  
		  BufferedReader access= new BufferedReader(fileReader);
           try 
           {  		   
               while ((line = access.readLine()) != null)
               { 
                   if (line !=null)
                   {
                  	   for (int i=0;i<3;i++) {System.out.println("\n");}
							GPSRecord =line.split(tupleInfo.getDelimiter());
						     //       line.split("\\"+tupleInfo.getDelimiter());							
						
                        if (tupleInfo.getFieldList().size() == GPSRecord.length)
                           {
                        	_collector.emit(new Values(GPSRecord)); 
                            //tupleInfo = new TupleInfo(GPSRecord);              
                           }                          
                   }          
               } */
	    
		int count=0;
		int ch=0;
		while(true){
			byte[] b3=new byte[3];
			try {				
				 sock.getInputStream().read(b3,0,3);

				if(b3==null){
					System.out.println("read First 3 byte from socket failed ! ");
					break;
				}else{
				ch=b3[0];}				
				//System.out.println("ch="+ch);
				int len=SocketJava.bytesToShort(b3, 1);
				//System.out.println("len="+len);
				byte[] bytelen= new byte[len];
				sock.getInputStream().read(bytelen);
				if(bytelen==null){
					System.out.println("read the second part from byte from socket failed ! ");
					break;
				}
				 
				String gpsString=SocketJava.DissectOneMessage(ch,bytelen);
				String[] GPSRecord=null;
				if(gpsString!=null){
					GPSRecord =gpsString.split(TupleInfo.getDelimiter());
				//System.out.print("GPS Record= :");
//				for(int i=0;i<GPSRecord.length;i++)
//				System.out.print(GPSRecord[i]+",");
				
				}else{
					break;
					}
				//System.out.print("  Size of GPS record = "+GPSRecord.length+"\n");
				 if (tupleInfo.getFieldList().size() == GPSRecord.length)
			      {
			   	_collector.emit(new Values(GPSRecord)); 
			      }
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
		}

							
	
  
       //tupleInfo = new TupleInfo(GPSRecord); 
        
              
//               if(line==-1){
//            	  System.out.println("Storm has reached the end of file /home/ghchen/GPS_2011_09_27.txt !");             	   
//               }
//          } 
//          catch (IOException ex) {
//        	  throw new RuntimeException("error:fail to read from file /home/ghchen/GPS_2011_09_27.txt",ex);  
 	  		
        
           
    }        

    @Override
    public void ack(Object id) {
    	System.out.println("OK:"+id);
    }
    

    @Override
    public void fail(Object id) {
    	System.out.println("Fail:"+id);
    }    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
 
    	TupleInfo tuple = new TupleInfo();
    	Fields fieldsArr;
    	try {
    		fieldsArr= tuple.getFieldList(); 
    		declarer.declare(fieldsArr);
			
		} catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException("error:fail to new Tuple object in declareOutputFields, tuple is null",e);  
		}    	  		

    }

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	static int count=0;
	public static void writeToFile(String fileName, Object obj){
		try {
			count=count+1;
			FileWriter fwriter;
			fwriter= new FileWriter(fileName,true);
		     BufferedWriter writer= new BufferedWriter(fwriter);
		      
		      	writer.write(count+": "+obj.toString());

		      writer.write("\n\n");
		      writer.close(); 
				
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}
	


    
}
