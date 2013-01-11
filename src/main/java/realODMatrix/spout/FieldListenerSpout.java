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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class FieldListenerSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
    private BufferedReader fileReader;
    private TopologyContext context;
    //private String file="/home/ghchen/2013-01-05.1/2013-01-05--11_05_48.txt";
    private TupleInfo tupleInfo= null;
    String[] fields=null;
    
    @Override
    public void close() {
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
		 String file=new String();
		 if(file.equals(""))
		 {
			file="/home/ghchen/2013-01-05.1/2013-01-05--11_05_48.txt";
			 //file="D:\\Dropbox\\172.20.7.126-id_rsa.pub";
		 }
		 
	try 
    	{	
		 this.context=context;

    	  this.fileReader = new BufferedReader(new FileReader(new File(file)));
    	} 
    	catch (FileNotFoundException e) 
    	{
    		throw new RuntimeException ("error reading file ["+file+"]");
    	    //System.exit(1);
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

						if (tupleInfo.getDelimiter().equals("|"))
                           fields = line.split("\\"+tupleInfo.getDelimiter());
                        else   fields = line.split(tupleInfo.getDelimiter());                                                
                        if (tupleInfo.getFieldList().size() == fields.length)
                           _collector.emit(new Values(fields)); 
                   }          
               } 
          } 
          catch (IOException ex) { }               
        //}    
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
       // declarer.declare(new Fields("word"));
//	String[] fieldsArr=new String [7];//tupleInfo.getFieldList().size()];
//        for(int i=0; i<tupleInfo.getFieldList().size(); i++)
//        {
//            fieldsArr[i] = tupleInfo.getFieldList().get(i).getClass().toString();//.getColumnName();
//        }  
    	TupleInfo tupleInfo = new TupleInfo(fields);    	
    	Fields fieldsArr= tupleInfo.getFieldList();
        declarer.declare(fieldsArr);

        System.out.println(fieldsArr);

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
