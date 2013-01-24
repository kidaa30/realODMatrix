/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 涓嬪崍1:57:46
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.spout;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.collections.map.StaticBucketMap;

import com.ibm.icu.text.DecimalFormat;
/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 1:57:46
 * email: gh.chen@siat.ac.cn
 *
 */
public class SocketJava {

	// TODO Auto-generated method stub
	private static final short POSINFO_PLATE_NUMBER			=		0x0001;
	private static final short POSINFO_LONGITUDE			=		0x0002;
	private static final short POSINFO_LATITUDE				=	0x0003;
	private static final short POSINFO_REPORT_TIME			=			0x0004;
	private static final short POSINFO_DEV_ID				=	0x0005;
	private static final short POSINFO_SPEED				=	0x0006;
	private static final short POSINFO_DIRECTION			=	0x0007;
	private static final short	POSINFO_LOCATION_STATUS		=				0x0008;

	private static final short ALARMINFO_SIM_NUMBER			=		0x0010;
	private static final short ALARMINFO_CAR_STATUS			=		0x0011;
	private static final short ALARMINFO_CAR_COLOUR			=		0x0012;


	static Socket sock ;
    static String GPSline=new String();
	static  String out=new String();
	
	public static void main(String[] args) throws Throwable    {
		sock=new Socket("172.20.14.204",15025);
		receive(sock);
		System.out.println("success !");
		
	}


	
	public static int receive(Socket sock) throws Exception { 
	
		System.out.println("#---------连接成功，数据接收中.........\n");
		System.out.println("#    .为一条位置信息\n");
		System.out.println("#    #为一条警报信息\n");
		System.out.println("#    *为一条营运数据\n");

		int count=0;
		while(true){
			byte[] b3=new byte[3];
			sock.getInputStream().read(b3,0,3);
			int ch=b3[0];

			int len=bytesToShort(b3, 1);
			byte[] bytelen= new byte[len];
			sock.getInputStream().read(bytelen);


 
			DissectOneMessage(ch,bytelen);
			//System.out.println(count++ +":\n");

		}	  

	}

	public static void DissectOneMessage(int ch,  byte[] msg) throws Exception{
		int len=msg.length;
		switch (ch) {
		case 0x02:
			//System.out.println("received GPS record ! len="+len+"\t");
			System.out.print(".");
			DissectPositionInfo(msg, msg.length);
			break;

		case 0x03:
			//System.out.println("received warning record ! len="+len+"\n");
			System.out.print("#");
			break;
		case 0x04:
			//System.out.println("received operation record ! len="+len+"\n");
			System.out.print("*");
			break;
		default:
			System.out.println("@@@		Unknow unit code, unit code="+ch+"		@@@\n");
			sock = new Socket("172.20.14.204", 15025);

			break;
		}	

	}


	public static String DissectPositionInfo(byte[] msg, int len) throws Exception
	{
		short offset=0;
		short unit_id = 0;
		short unit_len = 0;
		byte[] unit_value = null;

		String plate=null;
		int	 tempint=0;
		short tempshort1=0,tempshort2=0,tempshort3=0,tempshort4=0,tempshort5=0,tempshort6=0;

		char tempchar=0;

		while (offset<len)
		{
			unit_id =bytesToShort(msg,offset);

			offset = (short) (offset +2);

			unit_len = (short) bytesToShort(msg, offset);
			//System.out.println("	Unit len=: "+unit_len);
			
			if(unit_len+offset<=len){
				offset = (short) (offset + 2);

				unit_value=new byte[unit_len];
				for(int i=0;i<unit_len;i++) {

					unit_value[i]=msg[offset+i];
				}
				offset = (short) (offset + unit_len);
			}else {
				break;
			}
			
			DecimalFormat df2=(DecimalFormat) DecimalFormat.getInstance(); 

			

			switch (unit_id)
			{
			case POSINFO_PLATE_NUMBER:
				
				plate=new String(unit_value,"GBK");
				//System.out.println("	Plate number:"+plate+"\t");
				GPSline=plate+",";
				plate=null;
				break;
			case POSINFO_LONGITUDE:
				long lon=0;
				if(unit_len==2)
					tempint = bytesToShort(unit_value);
				else if(unit_len==4)
					lon = bytesToInt(unit_value);
				//System.out.println("	longitude:"+lon+"\t");
				double dLon=lon/1000000.0;
				df2.applyPattern("0.000000"); 
				GPSline=GPSline+df2.format(dLon)+",";
				break;
			case POSINFO_LATITUDE:
				long lan=0;
				if(unit_len==2)
					lan = bytesToShort(unit_value);
				else if(unit_len==4)
					lan = bytesToInt(unit_value);
				else if(unit_len==8)
					lan = bytesToLong(unit_value);


				//System.out.println("	latitude:"+tmp+"\t");
				double dLan=lan/1000000.0;
				df2.applyPattern("0.000000");
				GPSline=GPSline+df2.format(dLan)+",";
				break;
			case POSINFO_REPORT_TIME:				

				df2.applyPattern("00"); 
				//System.out.println(df2.format(1.2));
				
				tempshort1 =(short) bytesToShort(unit_value);

				tempshort2 = (short)unit_value[2] ;

				tempshort3 = (short) unit_value[3] ;

				tempshort4 =  (short) unit_value[4] ;

				tempshort5 =  (short) unit_value[5] ;

				tempshort6 = (short) unit_value[6] ;
				//System.out.println("	Date: "+tempshort1+"-"+tempshort2+"-"+tempshort3+"-"+tempshort4+"-"+tempshort5+"-"+tempshort6+"\t");
				GPSline=GPSline+tempshort1+"-"+df2.format(tempshort2)+"-"+df2.format(tempshort3)+
						" "+df2.format(tempshort4)+":"+df2.format(tempshort5)+":"+df2.format(tempshort6)+",";
				break;
			case POSINFO_DEV_ID:
				long sim =0;
				if(unit_len==2)
					sim = bytesToShort(unit_value);
				else if(unit_len==4)
					sim = bytesToInt(unit_value);
				else if(unit_len==8)
					sim= bytesToLong(unit_value);

				//System.out.println("	Device ID:"+tempint+"\t");
				GPSline=GPSline+sim+",";
				break;
			case POSINFO_SPEED:	

				tempshort1 = (short) bytesToShort(unit_value);
				//System.out.println("	Speed:"+tempshort1+"\t");
				GPSline=GPSline+df2.format(tempshort1) +",";
				break;
			case POSINFO_DIRECTION:	
				tempshort1 = (short) bytesToShort(unit_value);
				//System.out.println("	Bearing:"+tempshort1+"\t");
				//df2.applyPattern("000"); 
				GPSline=GPSline+(short)(tempshort1/100) +",";
				break;
			case POSINFO_LOCATION_STATUS:
				tempchar = (char) unit_value[0];
				tempshort1 = (short) tempchar;
				//System.out.println("	positioning status:"+tempshort1+"\t");
				//GPSline=GPSline+tempshort1 +",";
				break;
			case ALARMINFO_SIM_NUMBER:
				plate=new String(unit_value,"GBK");
				//System.out.println("	SIM NO.:"+plate+"\t");
				//GPSline=GPSline+plate+",";
				plate=null;
				break;
			case ALARMINFO_CAR_STATUS:
				tempchar = (char) unit_value[0];
				tempshort1 = (short) tempchar;
				//System.out.println("	With passenger:"+tempshort1+"\t");
				GPSline=GPSline+tempshort1 +",";
				break;
			case ALARMINFO_CAR_COLOUR:
				plate=new String(unit_value,"GBK");
				//System.out.println("	Car Color:"+plate+"\n");
				GPSline=GPSline+plate +"\n";
				plate=null;
				//System.out.println(GPSline);
				String[] newGps=GPSline.split(",");

				out=newGps[0]+","+newGps[3]+","+newGps[7]+","+newGps[5]+","+
				     newGps[6]+","+newGps[2]+","+newGps[1]+"\n";
				     
				//System.out.println(out);
				return out;
								
				break;
			default:
				System.out.println("### 	Error: can't resort message info!   #### unit_id=\n"+unit_id);
				
				break;

			}
		}
		return 0;
	}

public static String getGPSrecord() throws Throwable{
	receive(sock);
	
	return  out;
}





	public static short bytesToShort(byte[] b, int offset) {  
		return (short)    (b[offset + 1] & 0xff <<8 | (b[offset] & 0xff) << 0)   ; 
	}  	 

	public static short bytesToShort(byte[] b) {  
		return (short)( (b[1] & 0xff)<<8 | (b[0] & 0xff) );// << 8);  
	} 

	public static long bytesToLong(byte[] array) {  
		return ((((long) array[0] & 0xff) << 0) | (((long) array[1] & 0xff) << 8) | (((long) array[2] & 0xff) << 16)  
				| (((long) array[3] & 0xff) << 24) | (((long) array[4] & 0xff) << 32)  
				| (((long) array[5] & 0xff) << 40) | (((long) array[6] & 0xff) << 48) | (((long) array[7] & 0xff) <<56));  
	}  



	public static int bytesToInt(byte b[]) {  
		return (b[3] & 0xff )<<24 | (b[2] & 0xff )<< 16 | (b[1] & 0xff) << 8 | (b[0] & 0xff) << 0;  
	}    

}





