package realODMatrix.spout;

import java.util.List;

import backtype.storm.tuple.Fields;

public class TupleInfo {
	private String viechleID;
	private String dateTime;
	private Integer occupied;
	private Integer speed;
	private Integer bearing;
	private Double latitude;
	private Double longitude;

	private int numMember=7;
	
	
	public TupleInfo(){
		
	}
	
	public TupleInfo(List<String> input){
		int lstLength=input.size();
		if(numMember!=lstLength)
			System.out.print("input list size mismatch");// ("input list size mismatch");
		else 
		{
			 viechleID=input.get(0);	
			 dateTime=input.get(1);
			 latitude=Double.parseDouble(input.get(2));
			 longitude=Double.parseDouble(input.get(3));
			 speed=Integer.parseInt(input.get(4));
			 melostone=Double.parseDouble(input.get(5));
			 bearing=Integer.parseInt(input.get(6));			
		}
	}
	


/*	public String[] getFieldList() {
		// TODO Auto-generated method stub
		//Fields fieldList= new Fields(viechleID,dateTime,latitude,longitude,speed,melostone,bearing);
		String[] fieldList= new String[numMember];

		
			
		fieldList[0]=viechleID;
		fieldList[1]=dateTime;
		fieldList[2]=Double.toString(latitude) ;
		fieldList[3]=Double.toString(longitude) ;
		fieldList[4]=Integer.toString(speed) ;
		fieldList[5]=Double.toString(melostone);
		fieldList[6]=Integer.toString(bearing);		
		return fieldList;
	}*/
	
	
	public Fields getFieldList() {		
		//Fields fieldList= new Fields(viechleID,dateTime,latitude,longitude,speed,melostone,bearing);
		Fields fieldList= new Fields (viechleID,dateTime,Integer.toString(occupied),
				Integer.toString(speed),Integer.toString(bearing),Double.toString(latitude),
				Double.toString(longitude));		
		return fieldList;
	}




	public String getDelimiter() {
		// TODO Auto-generated method stub
		//String delimiter="|";
		String delimiter=",";
		return delimiter;
	
	}


}
