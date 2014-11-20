package config;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import system.Host;

public class Configuration {

	private static DocumentBuilder builder = null;

	public static Host MASTER = null;
	public static ArrayList<Host> WORKERS = new ArrayList<Host>();
	public static final int TIMEOUT = 30000;
	public static int NUM_OF_MAPPERS;
	public static int NUM_OF_REDUCERS;
	public static int NUM_OF_SPLITS;

	static {
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		try {
			builder = builderFactory.newDocumentBuilder();
			addDefaultResource("resources/config.xml");
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addDefaultResource(String filename) throws SAXException,
			IOException {
		File file = new File(filename);
		Document document = builder.parse(file);
		document.getDocumentElement().normalize();

		// master
		NodeList nList = document.getElementsByTagName("master");
		Node node = nList.item(0);
		if (node.getNodeType() == Node.ELEMENT_NODE) {
			Element element = (Element) node;
			MASTER = new Host(InetAddress.getByName(getChild("ip", element)),
					Integer.valueOf(getChild("port-for-client", element)),
					Integer.valueOf(getChild("port-for-worker", element)));
		}

		// workers
		nList = document.getElementsByTagName("worker");
		for (int i = 0; i < nList.getLength(); i++) {
			node = nList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element) node;
				Host worker = new Host(InetAddress.getByName(getChild("ip", element)), 
						Integer.valueOf(getChild("port-for-client", element)), 
						Integer.valueOf(getChild("port-for-worker", element)));
				WORKERS.add(worker);
			}
		}

		NUM_OF_MAPPERS = Integer.valueOf(getValue("num-of-mappers", document));
		NUM_OF_REDUCERS = Integer.valueOf(getValue("num-of-reducers", document));
		NUM_OF_SPLITS = Integer.valueOf(getValue("num-of-splits", document));
		
	}

	public static Host getWorkerByAddress(String ipAddress) {
		for (Host h : WORKERS) {
			if (h.getIPAddress().getHostAddress().equals(ipAddress))
				return h;
		}
		return null;
	}

	private static String getChild(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}
	
	private static String getValue(String tag, Document document) {
		return document.getElementsByTagName(tag).item(0).getChildNodes().item(0).getNodeValue();
	}
	
	public static void main(String[] args) throws SAXException, IOException {
//		Host h = getWorkerByAddress("128.237.191.70");
//		if (h != null)
//			System.out.println(h.getIPAddress().getHostAddress());
//		else
//			System.out.println("error");
	}
}
