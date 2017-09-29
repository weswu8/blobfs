package com.wesley.blobfs;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;

/* Factory to create multiple blob clients */ 
public final class BlobClientService {
	
	private static BlobClientService instance = new BlobClientService();
	private static Logger logger = LoggerFactory.getLogger("BlobClientService.class");
	private static CloudStorageAccount storageAccount;
	private static CloudBlobClient blobClient;
	
	private BlobClientService(){
		try {
        	/* Retrieve storage account from connection-string */
			storageAccount = CloudStorageAccount.parse(Constants.STORAGE_CONNECTION_STRING);
        }
        catch (IllegalArgumentException|URISyntaxException ex) {
        	logger.error(ex.getMessage());
        	ex.printStackTrace();
        }
        catch (InvalidKeyException ex) {
        	logger.error(ex.getMessage());
        	ex.printStackTrace();
        }
	};
	
	@SuppressWarnings("static-access")
	public static CloudBlobClient getBlobClient() throws Exception{
		return instance.createBlobClient();			
	}
	
	/**
	 * create the blob client
	 * @return
	 * @throws Exception
	 */
	private static CloudBlobClient createBlobClient () throws Exception{
		blobClient = null;
		try {
	        /* Create the blob client */
			blobClient = storageAccount.createCloudBlobClient();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
        	ex.printStackTrace();
		}		
        return blobClient;
		
	}

}
