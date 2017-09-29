package com.wesley.blobfs;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveSubscriptionMessageResult;


// Message used for synchronize the catch state across the nodes
public class MessageService {
	private static Logger logger = LoggerFactory.getLogger("BlobService.class");
	private static String sbTopicConnString = Constants.SERVICE_BUS_CONNECTION_STRING;
	private static String topic = Constants.SERVICE_BUS_TOPIC;
	private static String subscription = Constants.SERVICE_BUS_SUBSCRIPTION;
	private static ServiceBusContract  msgService;
	
	private MessageService(){
	};
	
	public final static ServiceBusContract getMsgService() throws Exception{
		if (msgService != null){
			return msgService;
		}else{
			return buildMsgService();
		}
	}
	
	public static ServiceBusContract buildMsgService () throws Exception{
		try {
			Configuration config = new Configuration();
			ServiceBusConfiguration.configureWithConnectionString(null, config, sbTopicConnString);
			return ServiceBusService.create(config);
		} catch (Exception ex) {
			logger.error("connection error: {}", ex.getMessage());
			throw ex;
		}
		
	}	
	
	public static boolean sbSendMessages (String msgBody) throws Exception{
		try {			
			ServiceBusContract mService = getMsgService();
			BrokeredMessage message = new BrokeredMessage(msgBody);
			mService.sendTopicMessage(topic, message);
		} catch (Exception ex) {
			logger.error("Exception occurred when sending the message :{} , {} ", msgBody, ex.getMessage());
			throw ex;
		}
		return true;
	}
	
	public static ArrayList<String> sbReceiveMessages () throws Exception{
		ArrayList<String> msgs = new ArrayList<String>();
		try {			
			ServiceBusContract mService = getMsgService();
			ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
		    opts.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);
		    while(true)  {
		    	ReceiveSubscriptionMessageResult  resultSubMsg = mService.receiveSubscriptionMessage(topic, subscription, opts);
		        BrokeredMessage message = resultSubMsg.getValue();
		        if (message != null && message.getMessageId() != null)
		        {
		        	StringBuilder msg = new StringBuilder();
                    byte[] b = new byte[200];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    while (-1 != numRead) {
                        s = new String(b);
                        msg.append(s.trim());
                        numRead = message.getBody().read(b);
                    }                    
		            msgs.add(msg.toString());
		        } else {
		            /* no messages */
		        	break;
		        }
		    }
			
		} catch (Exception ex) {
			logger.error("Exception occurred when receiving the message, {} ", ex.getMessage());
			throw ex;
		}
		return msgs;
	}
}
