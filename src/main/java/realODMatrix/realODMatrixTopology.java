/**
 * realODMatrix realODMatrix realODMatrixTopology.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 4:52:38
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

//import main.java.realODMatrix.bolt.CountBolt;
//import main.java.realODMatrix.bolt.DBWritterBolt;
//import main.java.realODMatrix.bolt.DistrictMatchingBolt;
import main.java.realODMatrix.spout.FieldListenerSpout;
import main.java.realODMatrix.spout.SocketSpout;
import main.java.realODMatrix.bolt.CountBolt;
import main.java.realODMatrix.bolt.CountBolt2;
import main.java.realODMatrix.bolt.CountBolt3;
import main.java.realODMatrix.bolt.DistrictMatchingBolt;


/**
 * realODMatrix realODMatrix realODMatrixTopology.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 4:52:38
 * email: gh.chen@siat.ac.cn
 *
 */
public class realODMatrixTopology  {

	public static void main(String[] args) throws AlreadyAliveException, 
													InvalidTopologyException, 
													InterruptedException {
		// TODO Auto-generated method stub
		
		FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();
		SocketSpout socketSpout=new SocketSpout();
		DistrictMatchingBolt districtMacthingBolt=new DistrictMatchingBolt(); 
		CountBolt3 countBolt3 =new CountBolt3();
		CountBolt2 countBolt2 =new CountBolt2();
//		DBWritterBolt dbWriterBolt = new DBWritterBolt();	
		
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        
	        //builder.setSpout("spout", fieldListenerSpout,1);	 
	        builder.setSpout("spout", socketSpout,1);	 
	        builder.setBolt("matchingBolt", districtMacthingBolt,5).shuffleGrouping("spout");	        
	       // builder.setBolt("countBolt",countBolt,6).shuffleGrouping("matchingBolt"); 
	        builder.setBolt("countBolt3",countBolt3,5).fieldsGrouping("matchingBolt",new Fields("districtID")); 
	        builder.setBolt("countBolt2",countBolt2,5).fieldsGrouping("matchingBolt",new Fields("districtID")); 
	        //builder.setBolt("dbBolt",dbWriterBolt,2).shuffleGrouping("countBolt");
		    Config conf = new Config();

            if(args.length==0){
		    	args=new String[1];
		    	args[0]="realOD";
            }	
		    
	        if(args!=null && args.length > 0) {
	            conf.setNumWorkers(16);            

	            //LocalCluster  cluster= new LocalCluster();
	            //cluster.submitTopology(args[0], conf, builder.createTopology());
	            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	        } 
	        else {     
	              
	              conf.setDebug(true);
	              conf.setMaxTaskParallelism(16);
	              LocalCluster cluster = new LocalCluster();
	              cluster.submitTopology("realOD", conf, builder.createTopology());
	    	      Thread.sleep(3000);
	    	      cluster.shutdown(); 
	        }

	    }

}

