/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 涓嬪崍1:57:46
 * email: gh.chen@siat.ac.cn
 */
package main.java.realODMatrix.bolt;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import main.java.realODMatrix.spout.SocketSpout;

//port backtype.storm.daemon.nimbus.newly_added_slots;





import com.vividsolutions.jts.index.bintree.Interval;

/**
 * realODMatrix main.java.realODMatrix.bolt test.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-1-15 1:57:46
 * email: gh.chen@siat.ac.cn
 *
 */
public class test {

	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
        Socket socket = null;   
        try {   
			int index = 0;   
			byte[] receive = new byte[4096];
			int count=0;
 
				Socket sock = new Socket("172.20.14.204", 15025);
				String str; 
				
		      
		        
				
				for(int i=0;i<2;i++){
					int ch=sock.getInputStream().read();
					char cha=(char)ch;
					System.out.println("ch= " + ch+"\n"); 
					switch (cha) {
					case 0x02:
						sock.getInputStream().read(receive,0,256);
						String s4096=new String(receive);
						System.out.println("s4096=: " + s4096+"\n"); 
						
						break;

					default:
						
						break;
					}
				}
				
//				for(int i=0;i<5;i++)/*while(true)*/{
//					sock.getInputStream().read(receive);           
//					str = new String(receive);
//					System.out.println("response: "+count++ + str+"\n"); 
//					//DissectOneMessage(receive);
//					//System.out.println(count++);
//					SocketSpout.DissectOneMessage(str);
//				}   
  
		}catch (Exception e) {   
			e.printStackTrace();   
		
		}    
    }
	
/*	
	Socket sock;
	try {
		sock = new Socket("172.20.14.204", 15025);
		//DataInputStream dis= new DataInputStream(sock.getInputStream());
		while(true){
			byte[] byte4096=new byte[4096];			
			sock.getInputStream().read(byte4096, 0, 4096);
			String ooString=new String(byte4096);
			System.out.println("byte4096=:"+byte4096.toString()+"\n");
			char ch =byte4096.toString().charAt(0);
			if(ch!=0x02 ||ch!=0x03) System.out.println("invalid char:"+ch+"\n");
			

		switch(ch){
		case 0x02:
			System.out.println("received GPS Record\n");
			byte[] byte2= new byte[2];
			int len=sock.getInputStream().read(byte2, 0, 2);
			System.out.println("GPS="+byte2+"\n");
			byte[] b= new byte[len];
			sock.getInputStream().read(b, 0, len);
			System.out.println("GPS:"+b.toString()+"\n");
			
			break;
		case 0x03:	
			System.out.println("warnig\n");
			break;
		}
		}
			
	} catch (UnknownHostException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();


	
	}*/
	
		
		
		//System.out.println("nowTime="+nowTime);
		//System.out.println(Interval);
	public void read(){
	File file = new File ("D:/aa.cer");
	InputStreamReader isr= new InputStreamReader (new FileInputStream (file), "utf-8");
	String line = null;
	while ((line = bf.readLine ())!= null){
	    System.out.print (line);
	}


	}


	


