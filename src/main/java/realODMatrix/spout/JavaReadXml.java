package main.java.realODMatrix.spout;

// ganbo 2013-01-14


import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;





public class JavaReadXml {

	private Document doc = null;  
	Element root = null ;
	public JavaReadXml() {
		super();
		// TODO Auto-generated constructor stub
	}

    public JavaReadXml(String xmlFile)  throws Exception{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();    
			DocumentBuilder db = dbf.newDocumentBuilder();
			doc = db.parse(new File(xmlFile));
		}
		


    public LinkedList<String> getFieldList() {
    	Element root = doc.getDocumentElement();
    	NodeList children = root.getElementsByTagName("COLUMNNAME");
		LinkedList<String> ll = new LinkedList<String>();
		for(int i=0;i<children.getLength();i++){
			ll.add(children.item(i).getTextContent().trim());
		}
		return ll;
	}
    
    public int getTupleLength() {	
    	Element root = doc.getDocumentElement();
    	NodeList children = root.getElementsByTagName("COLUMNNAME");
    	return children.getLength();    	
    }
    
    
    public String getCloType(String colName){
    	HashMap< String, String> hMap =new HashMap<String, String>();
    	Element root = doc.getDocumentElement();
//    	Element root2 = doc.getDocumentElement();
    	NodeList children = root.getElementsByTagName("COLUMNNAME");
		NodeList children2 = root.getElementsByTagName("COLUMNTYPE");
    	for(int i=0;i<children.getLength();i++){
    		hMap.put(children.item(i).getTextContent().trim(), children2.item(i).getTextContent().trim());
    	}
		return hMap.get(colName);
    	
    }
    
    public String getDelimiter(){
    	Element root1 = doc.getDocumentElement();
    	NodeList   children= root1.getElementsByTagName("DELIMITER");
    	return children.item(0).getTextContent().trim();
    }
    
	public static void main(String[] args) throws Exception {
		JavaReadXml  jrx = new JavaReadXml("src\\main\\java\\realODMatrix\\spout\\tupleInfo.xml");
		LinkedList<String> ll =   jrx.getFieldList();
		for(String s : ll){
			System.out.println(s);
		}
		 jrx.getDelimiter();
		 System.out.println(jrx.getCloType("bearing"));
		 System.out.println(jrx.getDelimiter());
		 
		 jrx.getTupleLength();
		 System.out.println(jrx.getTupleLength());
		 
	}
	
}
