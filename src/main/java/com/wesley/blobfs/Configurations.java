package com.wesley.blobfs;

import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public final class Configurations {
	
	private static Logger logger = LoggerFactory.getLogger("Configurations.class");
	private Properties properties = null;
	private static Configurations instance = null;
	
	/** Private constructor 
	 * @throws Exception */
	private Configurations () {
	    this.properties = new Properties();
	    try{
	    	properties.load(new FileInputStream(Constants.CONFIGURATION_FILE));

	    }catch(Exception e){
	    	logger.error("Failed to load the configuration file: {}.", "blobfs.conf");
	    }
	}   
	
	/** Creates the instance is synchronized to avoid multithreads problems */
	private synchronized static void createInstance () {
	    if(instance == null){
            synchronized (Configurations.class) {
                if(instance == null){
                    instance = new Configurations();
                }
            }
        }
	}
	
	/** Get a property of the property file, Uses singleton pattern */
	public static String getProperty(String key){
	    String result = null;
	    if(instance == null) {
	        createInstance();
	    }
	    if(key !=null && !key.trim().isEmpty()){
	        result = instance.properties.getProperty(key);
	    }
	    return result;
	}
	
	/** Override the clone method to ensure the "unique instance" requirement of this class */
	public Object clone() throws CloneNotSupportedException {
	    throw new CloneNotSupportedException();
	}
}
