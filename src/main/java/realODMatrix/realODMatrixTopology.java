/**
 * realODMatrix realODMatrix realODMatrixTopology.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-8 4:52:38
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import main.java.realODMatrix.bolt.CountBolt;
import main.java.realODMatrix.bolt.DistrictMatchingBolt;
//import main.java.realODMatrix.bolt.CountBolt;
//import main.java.realODMatrix.bolt.DBWritterBolt;
//import main.java.realODMatrix.bolt.DistrictMatchingBolt;
import main.java.realODMatrix.spout.FieldListenerSpout;

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
		
		DistrictMatchingBolt districtMacthingBolt=new DistrictMatchingBolt(); 
		CountBolt countBolt =new CountBolt();
//		DBWritterBolt dbWriterBolt = new DBWritterBolt();	
		
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        
	        builder.setSpout("spout", fieldListenerSpout);//,2);	        
	        builder.setBolt("matchingBolt", districtMacthingBolt).shuffleGrouping("spout");
	        
	        builder.setBolt("countBolt",countBolt).shuffleGrouping("matchingBolt"); 
//	        builder.setBolt("dbBolt",dbWriterBolt,2).shuffleGrouping("countBolt");


		    Config conf = new Config();
	        if(args!=null && args.length > 0) {
	            conf.setNumWorkers(3);            
	            //StormSubmitter.
	            LocalCluster  cluster= new LocalCluster();
	            cluster.submitTopology(args[0], conf, builder.createTopology());
	        } 
	        else {     
	              
	              conf.setDebug(true);
	              conf.setMaxTaskParallelism(3);
	              LocalCluster cluster = new LocalCluster();
	              cluster.submitTopology(
	              "Threshold_Test", conf, builder.createTopology());
	    	      Thread.sleep(3000);
	    	      cluster.shutdown(); 
	        }

	    }

}

