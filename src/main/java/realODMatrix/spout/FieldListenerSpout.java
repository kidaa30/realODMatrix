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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class FieldListenerSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
    private BufferedReader fileReader;
    //private TopologyContext context;
    //private String file="/home/ghchen/2013-01-05.1/2013-01-05--11_05_48.txt";
    private TupleInfo tupleInfo=new TupleInfo();
    String[] GPSRecord=null;
    //Fields fields;

    
    @Override
    public void close() {
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
		 String file=new String();
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
    	}
    }

    @Override
    public void nextTuple() {    	
   
        Utils.sleep(2000);
       // RandomAccessFile access = null; 
        String line = null;  
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
               } 
          } 
          catch (IOException ex) {
        	  throw new RuntimeException("error:fail to new Tuple object in declareOutputFields, tuple is null",ex);  
  		}    	  		
        
           
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

		      writer.write("\n");
		      writer.close(); 
				
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}


    
/*    public static void main(String[] args) {    	
    	FieldListenerSpout field = new FieldListenerSpout();
    	OutputFieldsDeclarer declarer;
    	Map conf = new HashMap();
    	conf.put(Config.TOPOLOGY_WORKERS, 4);
    	TopologyContext context;
    	SpoutOutputCollector collector;    	
    	
    	field.open(conf,context,collector);
    	field.nextTuple();    
		field.declareOutputFields(declarer);  
				
	}*/

    
}
