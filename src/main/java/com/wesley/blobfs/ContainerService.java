package com.wesley.blobfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wesley.blobfs.utils.BfsUtility;
import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public final class ContainerService {
	
	private static Logger logger = LoggerFactory.getLogger("BlobService.class");
	private static ContainerService instance = new ContainerService();
	static CloudBlobClient blobClient;
	
	@SuppressWarnings("static-access")
	private ContainerService() {
		try {
			instance.blobClient = BlobClientService.getBlobClient();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
        	ex.printStackTrace();
		}
	}
	
	/* get all the container within the storage account */
	public final static List<String> getAllContainersName() throws BlobfsException{
		
		List<String> containersNameList = new ArrayList<String>();
		Iterable<CloudBlobContainer> allContainers = blobClient.listContainers();
		try {
			for (CloudBlobContainer c : allContainers) {
				containersNameList.add(c.getName());
			} 
		} catch (Exception ex) {
			/* will verify the connection string */
			String errMessage = "Can not retrieve the containers, please verify the storage account connection string. " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return containersNameList;
		
	}
	
	/* get the private container reference */
	public final static CloudBlobContainer getPrivateContainer(String containerName) 
			throws BlobfsException{
	
		CloudBlobContainer privateContainer = null;
		try {
			privateContainer = blobClient.getContainerReference(containerName);
			if (!privateContainer.exists()){
				String errMessage = "The specified container: " + containerName + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when reading the container: " + containerName + ". " + ex.getMessage();
			//logger.error(errMessage);
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return privateContainer;
	}
	
	/* create the container  */
	public final static boolean createContainer(String containerName, ContainerPermissionType containerPermType) 
			throws Exception{
		boolean result = false;
		CloudBlobContainer container = null;
		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists();			
			/* set the permission */
			BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
			switch (containerPermType.toString()){
				case "PUBLIC":
					containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
					break;
				case "BLOB":
					containerPermissions.setPublicAccess(BlobContainerPublicAccessType.BLOB);
					break;
				case "PRIVATE":
					containerPermissions.setPublicAccess(BlobContainerPublicAccessType.OFF);
					break;
				default:
					String errMessage = "The container permission type: " + containerPermType + " is invalid.";
					throw new BlobfsException(errMessage);		
			}
			container.uploadPermissions(containerPermissions);
			result = true;
		} catch (Exception ex) {
			String errMessage = "Exception occurred when reading the container: " + containerName + ". " + ex.getMessage();
			//logger.error(errMessage);
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* delete the container */
	public final static boolean deleteContainer(String containerName) throws Exception{
		boolean result = false;
		CloudBlobContainer container = null;
		try {
			container = blobClient.getContainerReference(containerName);
			container.deleteIfExists();	
			result = true;
		} catch (Exception ex) {
			String errMessage = "Exception occurred when reading the container: " + containerName + ". " + ex.getMessage();
			//logger.error(errMessage);
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* check if the container exists */
	public final static boolean containerExists(String containerName) throws Exception{
		boolean result = false;
		CloudBlobContainer container = null;
		try {
			container = blobClient.getContainerReference(containerName);
			result = container.exists();			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when checking the container: " + containerName + ". " + ex.getMessage();
			//logger.error(errMessage);
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* Getting the properties of the container */
	public final static ContainerProperties getContainerProperties (String containerName) throws Exception {
		ContainerProperties containerProperties = new ContainerProperties();
		CloudBlobContainer container = null;
		try {
			container = blobClient.getContainerReference(containerName);
			container.downloadAttributes();
			containerProperties.setName(container.getName());
			containerProperties.setCreated(container.getProperties().getLastModified());
			containerProperties.setLastModified(container.getProperties().getLastModified());
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the container: " + containerName + " properties. " + ex.getMessage(); 
		    throw new BlobfsException(errMessage);
		}		
		return containerProperties;	
	}
	/* set the meta data of the container */
	public final static void setContainerMetadata (CloudBlobContainer containerInstance, String containerName, String key, String value) throws Exception {
		CloudBlobContainer container = null;
		try {
			if (null == containerInstance){
				container = blobClient.getContainerReference(containerName);
			} else {
				container = containerInstance;
			}
			container.downloadAttributes();
			HashMap<String, String> metadata = container.getMetadata();
		    if (null == metadata) {
		      metadata = new HashMap<String, String>();
		    }
		    metadata.put(key, value);
		    container.setMetadata(metadata);
		    /* upload the meta data to blob service */
		    container.uploadMetadata(AccessCondition.generateEmptyCondition(), null, null);
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the container: " 
    				+ containerName + ". key:" + key + " . value: " +value + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		
	}
	/* get the meta data of the container */
	public final static String getContainerMetadata (CloudBlobContainer containerInstance, String containerName, String... keyAlternatives) throws Exception {
		CloudBlobContainer container = null;
		try {
			if (null == containerInstance){
				container = blobClient.getContainerReference(containerName);
			} else {
				container = containerInstance;
			}
			container.downloadAttributes();
			HashMap<String, String> metadata = container.getMetadata();
		    if (null == metadata) {
		      return null;
		    }
		    for (String key : keyAlternatives) {
		      if (metadata.containsKey(key)) {
		        return metadata.get(key);
		      }
		    }
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the container: " 
    				+ containerName + ". keys: " + keyAlternatives.toString() +  ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	    return null;
	}
	/* remove the meta data of the container */	
	public final static String removeContainerMetadata (CloudBlobContainer containerInstance, String containerName, String key) throws Exception {
		CloudBlobContainer container = null;
		try {
			if (null == containerInstance){
				container = blobClient.getContainerReference(containerName);
			} else {
				container = containerInstance;
			}
			container.downloadAttributes();
			HashMap<String, String> metadata = container.getMetadata();
		    if (metadata != null) {
		    	 if (metadata.containsKey(key)) {
			        return metadata.get(key);
			     }
		    	 container.setMetadata(metadata);
			     /* upload the meta data to blob service */
		    	 container.uploadMetadata();
		    }		 
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when remove metadata of the blob: " 
    				+ containerName + ". key: " + key + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	    return null;
	}
}
