package com.wesley.blobfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.LeaseStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;
import com.wesley.blobfs.utils.BfsUtility;


@SuppressWarnings("static-access")
public final class BlobService {
	
	private static Logger logger = LoggerFactory.getLogger("BlobService.class");
	private static BlobService instance = new BlobService();
	private static CloudBlobContainer container;
	private BlobService(){};
	private static CloudBlob blob;

	/* Get the block and append blob reference */
	/**
	 * @param reqParams: container, blobType, blob
	 * @return
	 * @throws BlobfsException
	 */
	public final static CloudBlob getBlobReference (BlobReqParams reqParams) throws BlobfsException{
		instance.blob = null;
		String containerName = reqParams.getContainer();
		String blobType = (null != reqParams.getBfsBlobType()) ? reqParams.getBfsBlobType().toString() : "";
		String blobName = reqParams.getBlob();
		try {
			container = ContainerService.getPrivateContainer(containerName);
			switch (blobType){
				case "BLOCKBLOB":
				case "VIRTUALDIRECTORY":
					instance.blob = (CloudBlockBlob)container.getBlockBlobReference(blobName);
					break;	
				case "APPENDBLOB":
					instance.blob = (CloudAppendBlob)container.getAppendBlobReference(blobName);
					break;
				case "PAGEBLOB":
					instance.blob = (CloudPageBlob)container.getPageBlobReference(blobName);
					break;
				default:
					instance.blob = (CloudBlob)container.getBlobReferenceFromServer(blobName);
					break;	
			}
		} catch (Exception ex) {
			String errMessage = "Exception occurred when geting the blob reference: " + blobName + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return instance.blob;		
	}
	
	/* this function will create a blob object with zero size*/
	/**
	 * @param reqParams:container, blob
	 * @throws BlobfsException
	 */
	public final static boolean createVirtualDirectory (BlobReqParams reqParams) throws BlobfsException{
		boolean result = false;
		reqParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
		String vdir = reqParams.getBlob() + Constants.PATH_DELIMITER + Constants.VIRTUAL_DIRECTORY_NODE_NAME;
		reqParams.setBlob(vdir);
		try {
			CloudBlockBlob blob = (CloudBlockBlob) getBlobReference(reqParams);
			if (blob.exists()){
				String errMessage = "The specified directory: " + reqParams.getBlobFullPath()+ " already exists. ";
				throw new BlobfsException(errMessage);
			}
			blob.uploadText("","UTF-8",null, null, null);
			result = true;
			logger.trace("The specified directory:{} has been created.", reqParams.getBlobFullPath());
		} catch (Exception ex) {
			String errMessage = "Exception occurred when creating the directory: " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	/* get the blobs within the virtual directory */
	/**
	 * @param reqParams：virtualDirOptMode
	 * @return
	 * @throws BlobfsException
	 */
	public final static List<BfsBlobModel> getBlobsWithinVirtualDirectory (BlobReqParams reqParams) throws BlobfsException{
		List<BfsBlobModel> bfsBlobsList = new ArrayList<>();
		String sourceContainer = reqParams.getContainer();
		/* we should put "" or add "/" at the end of blob name */
	    String sourceBlob = (null == reqParams.getBlob() 
	    					|| "".equals(reqParams.getBlob())) ? "" : reqParams.getBlob() + "/";
		boolean vDirFlatMode = false;
		boolean getBlobProps = false;
		/* use the flat mode here */
		if ("FBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString()) 
				|| "FLAT".equals(reqParams.getVirtualDirOptMode().toString())){
			vDirFlatMode = true;
		}
		/* get the blob properties or not */
		if ("FBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString()) 
				|| "HBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString())){
			getBlobProps = true;
		}	
	    try {
	    	/* does not check whether the destination directory exists or not */
			CloudBlobContainer container = ContainerService.getPrivateContainer(sourceContainer);
			/* list the blob in hierarchical mode, flat mode should set the 2nd parameter to true 
			 * the blobItem.getName() will return this format :"sourceBlob/itemname"
			 * */
			Iterable<ListBlobItem> collection = container.listBlobs(sourceBlob, vDirFlatMode);
			for (ListBlobItem blobItem : collection) {
				 /* the blob item is a blob */
				 BfsBlobType bfsBlobType = null;
				 String sourceBlobName = null;
				 String blobURI = null;
				 if (blobItem instanceof CloudBlob){					 
					 if (blobItem instanceof CloudBlockBlob) {
						sourceBlobName = ((CloudBlockBlob) blobItem).getName();
						blobURI = ((CloudBlockBlob) blobItem).getUri().toString();
						bfsBlobType = BfsBlobType.BLOCKBLOB;				 
					 }else if(blobItem instanceof CloudAppendBlob){
						sourceBlobName = ((CloudAppendBlob) blobItem).getName();
						blobURI = ((CloudAppendBlob) blobItem).getUri().toString();
						bfsBlobType = BfsBlobType.APPENDBLOB;
					 }else if(blobItem instanceof CloudPageBlob){
						 sourceBlobName = ((CloudPageBlob) blobItem).getName();
						 blobURI = ((CloudPageBlob) blobItem).getUri().toString();
						 bfsBlobType = BfsBlobType.PAGEBLOB;
					 }
					 /* don't support other type yet */
					 if (null == sourceBlobName) {continue;}
					 /* fill the BfsBlobModel */
					 BfsBlobModel bfsBlobModel = new BfsBlobModel();
					 bfsBlobModel.setBfsBlobType(bfsBlobType);
					 bfsBlobModel.setBlobName(sourceBlobName);
					 bfsBlobModel.setBlobURI(blobURI);
					 /* get the blob properties, can think lazy mode later */
					 BlobProperties blobProperties = new BlobProperties();
					 if (getBlobProps){
						 BlobReqParams propReq = new BlobReqParams();
						 propReq.setBfsBlobType(bfsBlobType);
						 propReq.setBlob(sourceBlobName);
						 propReq.setContainer(sourceContainer);
						 blobProperties = getBlobProperties(propReq);
					 }
					 bfsBlobModel.setBlobProperties(blobProperties);
					 /* add to the list */
					 bfsBlobsList.add(bfsBlobModel);
				
				 }/* end of if if (blobItem instanceof CloudBlob) */
				 /* the blob item is a virtual directory */
				 if (blobItem instanceof CloudBlobDirectory){
					 //sourceBlobName = ((CloudBlobDirectory) blobItem).getPrefix();
					 /* for fuse issue: the dir end with slash will cause the fuse error */
					 sourceBlobName = BfsUtility.removeLastSlash(((CloudBlobDirectory) blobItem).getPrefix());
					 blobURI = ((CloudBlobDirectory) blobItem).getUri().toString();
					 bfsBlobType = BfsBlobType.VIRTUALDIRECTORY;
					 /* fill the BfsBlobModel */
					 BfsBlobModel bfsBlobModel = new BfsBlobModel();
					 bfsBlobModel.setBfsBlobType(bfsBlobType);
					 bfsBlobModel.setBlobName(sourceBlobName);
					 bfsBlobModel.setBlobURI(blobURI);
					 BlobProperties blobProperties = new BlobProperties();
					 bfsBlobModel.setBlobProperties(blobProperties);
					 /* add to the list */
					 bfsBlobsList.add(bfsBlobModel);					 
				 }
				 //System.out.println(sourceBlobName);
	
		    }/* end of for primary loop */
	    } catch (Exception ex) {
			String errMessage = "Exception occurred when listing the directory: " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return bfsBlobsList;
	}
	/* rename the virtual directory*/
	/**
	 * @param failedFilesList: store the failed file
	 * @param reqParams: blobOptMode, container, blob, destContainer, destBlob,
	 * @return
	 * @throws Exception 
	 */
	public final static boolean CopyOrmoveVirtualDirectory(List<String> failedFilesList, BlobReqParams reqParams) throws BlobfsException{
		boolean result = false;
		/* move or copy the virtual directory, default is copy */
		String sourceContainer = reqParams.getContainer();
		String sourceBlobDir = reqParams.getBlob();
		String destContainer = reqParams.getDestContainer();
		String destBlobDir = reqParams.getDestBlob();
		int retryTimes = Constants.DEFAULT_BLOB_OPRATION_RETRY_TIMES;;
		/* the source prefix and the destination prefix should be different, we don't check this */
		try {
			/* check whether the source directory exists or not */
			/* check whether the destination directory exists or not, this should be done in the caller */
			BlobReqParams checkReq = new BlobReqParams();
			checkReq.setContainer(destContainer);
			checkReq.setBlob(destBlobDir);
			if (virtualDirectoryExists(checkReq)){
				String errMessage = "Exception occurred when renaming the directory from " + reqParams.getBlobFullPath() + " to " +
						reqParams.getDestBlobFullPath() + ". The specified directory already exists.";
				throw new BlobfsException(errMessage);
			}
			/* get the blobs within the virtual directory with lazy mode */
			BlobReqParams getBlobsReq = new BlobReqParams();
			getBlobsReq.setContainer(sourceContainer);
			getBlobsReq.setBlob(sourceBlobDir);
			/* use flat mode here, there is no virtual directory in this mode 
			 * safe copy/move , delete the original file only if copy/move successed*/
			getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.FLAT);
			List<BfsBlobModel> bfsBlobModels = new BlobService().getBlobsWithinVirtualDirectory(getBlobsReq);
			for(BfsBlobModel bfsBlobModel: bfsBlobModels){
				 int retryCount = 1;
				 String sourceBlobName = bfsBlobModel.getBlobName();
				 BfsBlobType bfsBlobType = bfsBlobModel.getBfsBlobType();
				 String destBlobName;
				 if ("".equals(sourceBlobDir.trim())){
					 destBlobName = destBlobDir + Constants.PATH_DELIMITER + sourceBlobName;
				 } else {
					destBlobName = sourceBlobName.replace(sourceBlobDir, destBlobDir);
				 }	
				 BlobReqParams moveReq = new BlobReqParams();
				 /* use copy mode for the directory copy/move */
				 moveReq.setBlobOptMode(BlobOptMode.COPY);
				 moveReq.setBfsBlobType(bfsBlobType);
				 moveReq.setContainer(sourceContainer);
				 moveReq.setBlob(sourceBlobName);
				 moveReq.setDestContainer(destContainer);
				 moveReq.setDestBlob(destBlobName);
				 moveReq.setDoForoce(true);
				 while(!CopyOrmoveBlobCrossContainer(moveReq)){
					 if (retryCount == retryTimes) {
						 failedFilesList.add(bfsBlobModel.getBlobURI());
						 logger.trace("Exception occurred when renaming the directory from {} to {}."
									, reqParams.getBlobFullPath(), reqParams.getDestBlobFullPath());
						 break;
					 }
					 retryCount ++ ;
					 Thread.sleep(Constants.DEFAULT_THREAD_SLEEP_MILLS);
				 }/* end of while */				
				 /* rename the blob successfully */
				 if (retryCount < retryTimes){
					logger.trace("Rename successed. from {} to {}.", moveReq.getBlobFullPath(), moveReq.getDestBlobFullPath() );
				 }
			}
			/* copy/move successfully, we need delete the original file for move opt mode */
			if (failedFilesList.isEmpty()){
				if ("MOVE".equals(reqParams.getBlobOptMode().toString())){
					List<String> delFailedFiles = new ArrayList<String>();
					deleteVirtualDirectory(delFailedFiles, reqParams);
					if (delFailedFiles.isEmpty()){ result = true; }
				} else {
					result = true;
				}
			} 			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when renaming the directory from " + reqParams.getBlobFullPath() + " to " +
					reqParams.getDestBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		
		return result;
	}
	/* delete the virtual directory*/
	/**
	 * @param failedFilesList
	 * @param reqParams: container, blob
	 * @return
	 * @throws Exception
	 */
	public final static boolean deleteVirtualDirectory(List<String> failedFilesList, BlobReqParams reqParams) throws BlobfsException{
		boolean result = false;
		String sourceContainer = reqParams.getContainer();
		String sourceBlobDir =  reqParams.getBlob();
		int retryTimes = Constants.DEFAULT_BLOB_OPRATION_RETRY_TIMES;
		try {
			/* does not check whether the destination directory exists or not */
			/* get the blobs within the virtual directory with lazy mode */
			BlobReqParams getBlobsReq = new BlobReqParams();
			getBlobsReq.setContainer(sourceContainer);
			getBlobsReq.setBlob(sourceBlobDir);
			/* use flat mode here, there is no virtual directory in this mode */
			getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.FLAT);
			List<BfsBlobModel> bfsBlobModels = new BlobService().getBlobsWithinVirtualDirectory(getBlobsReq);
			for(BfsBlobModel bfsBlobModel: bfsBlobModels){
				 int retryCount = 1;
				 String sourceBlobName = bfsBlobModel.getBlobName();
				 BfsBlobType bfsBlobType = bfsBlobModel.getBfsBlobType();
				 BlobReqParams delReq = new BlobReqParams();
				 delReq.setBfsBlobType(bfsBlobType);
				 delReq.setContainer(sourceContainer);
				 delReq.setBlob(sourceBlobName);
				 delReq.setDoForoce(true);
				 while(!deleteBlob(delReq)){
					 if (retryCount == retryTimes) {
						 failedFilesList.add(bfsBlobModel.getBlobURI());
						 logger.trace("Exception occurred when deleting the directory: {}.", delReq.getBlobFullPath());
						 break;
					 }
					 retryCount ++ ;
					 Thread.sleep(Constants.DEFAULT_THREAD_SLEEP_MILLS);
				 }/* end of while */				
				 /* delete the blob successfully */
				 if (retryCount < retryTimes){
					logger.trace("Delete successed: {}.", delReq.getBlobFullPath());
				 }
			}
			result = true;			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when deleting the directory from " + reqParams.getBlobFullPath() + " to " +
					reqParams.getDestBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* upload the file with single part */
	/**
	 * @param reqParams: localDir, LocalFile, container, blob, doForoce
	 * @throws BlobfsException
	 */
	public final static void uploadBlockBlobWithSinglePart (BlobReqParams reqParams) throws BlobfsException{
		String srcLocalFileFullPath = reqParams.getLocalFileFullPath();
		String destBlob = reqParams.getBlob();
		boolean doForce = reqParams.isDoForoce();
		FileInputStream srcStream = null;
		long startTime = 0;
		try {
			/* check the source file */
			File srcFile = new File(srcLocalFileFullPath);
			if (!srcFile.exists()){
				String errMessage = "The source file:" + srcLocalFileFullPath + " does not exist.";
				throw new BlobfsException(errMessage);
			}				
			srcStream = new FileInputStream(srcFile);
			reqParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			CloudBlockBlob blob = (CloudBlockBlob) getBlobReference(reqParams);
			if (blob.exists() & !doForce){
				String errMessage = "The target blob:" + reqParams.getBlobFullPath() + " already exists.";
				throw new BlobfsException(errMessage);
			}
			/* set the md5 value ,azure blob will generate the Content-MD5 value for put and update block object*/
	        /* warning, the md5 value may be modified by other apps, so we will not support md5 up to now*/
			/* set the request options */
			BlobRequestOptions blobReqOpts = new BlobRequestOptions();
			blobReqOpts.setConcurrentRequestCount(Constants.CONCURRENT_REQUEST_COUNT);
			blobReqOpts.setSingleBlobPutThresholdInBytes(Constants.SINGLE_BLOB_PUT_THRESHOLD); //16 MB
			blobReqOpts.setRetryPolicyFactory(new RetryExponentialRetry(Constants.UPLOAD_BACKOFF_INTERVAL, Constants.UPLOAD_RETRY_ATTEMPTS));
			blob.setStreamWriteSizeInBytes(Constants.STREAM_WRITE_SIZE); //4 MB
			/*  AccessCondition does not work with upload function */
			startTime = System.currentTimeMillis();
			logger.trace("Start uploading, from {} to {} ,The size is {}. ",srcLocalFileFullPath, reqParams.getBlobFullPath(), srcFile.length());
			blob.upload(srcStream, srcFile.length(), null, blobReqOpts, null);
			//blob.uploadFromFile(sourcePath, accessCondition, blobReqOpts, null);
			srcStream.close();
			long totalTime = System.currentTimeMillis() - startTime;
			
			/* refresh the properties */
			blob.downloadAttributes();
			logger.trace("Upload completed, from {} to {} ,Bytes Uploaded: {}, The time spent is {} ms.",
					srcLocalFileFullPath, destBlob, blob.getProperties().getLength(), totalTime);
		} catch (Exception  ex) {
			 String errMessage = "Exception occurred when upload the file:" + srcLocalFileFullPath + " to " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			 BfsUtility.throwBlobfsException(ex, errMessage);
		 } finally {
			 if (null != srcStream){
				 try{
		        	srcStream.close();
			     }catch (Exception ex){
			    	 String errMessage = "Exception occurred when close the file:" + srcLocalFileFullPath + ". " + ex.getMessage();
			    	 BfsUtility.throwBlobfsException(ex, errMessage);

			     }
			  }
		 }													
	}
	/*upload the file with multiple parts */
	/**
	 * @param reqParams: localDir, LocalFile, container, blob, doForoce
	 * @throws BlobfsException
	 */
	public final static void uploadBlockBlobWithMultipleParts (BlobReqParams reqParams) throws BlobfsException{
		String srcLocalFileFullPath = reqParams.getLocalFileFullPath();
		boolean doForce = reqParams.isDoForoce();
		FileInputStream srcStream = null;
		long startTime = 0;
		long totalTime = 0;
		try {
			/* check the source file */
			File srcFile = new File(srcLocalFileFullPath);
			if (!srcFile.exists()){
				String errMessage = "The source file:" + srcLocalFileFullPath + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			/* check the size of the file */
			if ((long)srcFile.length() > Constants.BLOCKBLOB_SIZE_LIMIT){ //5TB
			    String errMessage = "The size of target blob: " + reqParams.getBlobFullPath() + " exceeds the 5TB size limit.";
				throw new BlobfsException(errMessage);
			}
			srcStream = new FileInputStream(srcFile);
			reqParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			CloudBlockBlob blob = (CloudBlockBlob) getBlobReference(reqParams);
			if (blob.exists() & !doForce){
				String errMessage = "The target blob:" + reqParams.getBlobFullPath() + " already exists.";
				throw new BlobfsException(errMessage);
			}
     		/* set counters */
            long srcfileSize = (long)srcFile.length();
            int blockSize = Constants.UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE;
            int blockCount = (int)((float)srcfileSize / (float)blockSize) + 1;
            long bytesLeft = srcfileSize;
            int blockNumber = 0;
            //long bytesRead = 0;
            
			/* list of all block ids we will be uploading - need it for the commit at the end */
            List<BlockEntry> blockList = new ArrayList<BlockEntry>();
            
            /* set the request options */
			BlobRequestOptions blobReqOpts = new BlobRequestOptions();
			//blobReqOpts.setConcurrentRequestCount(4);
			startTime = System.currentTimeMillis();
			logger.trace("Start uploading, from {} to {} ,The size is {}. ",srcLocalFileFullPath, 
					reqParams.getBlobFullPath(), srcfileSize);
               
			startTime = System.currentTimeMillis();
            /* loop through the file and upload chunks of the file to the blob */
            while( bytesLeft > 0 ) {
            	
                blockNumber++;

                /* how much to read (only last chunk may be smaller) */
                int bytesToRead = 0;
                if ( bytesLeft >= (long)blockSize ) {
                  bytesToRead = blockSize;
                } else {
                  bytesToRead = (int)bytesLeft;
                }

                /*  trace out progress */
                float percentageDone = ((float)blockNumber / (float)blockCount) * (float)100;
				logger.trace("The source file: {} , blockId: {}. {}% done", srcLocalFileFullPath, blockNumber, percentageDone);

                /* save block id in array (must be base64) */
                String blockId = Base64.getEncoder().encodeToString(String.format("BlockId%07d", blockNumber).getBytes(StandardCharsets.UTF_8));
                BlockEntry block = new BlockEntry(blockId);
                blockList.add(block);

                /*  upload block chunk to Azure Storage */
                //blob.uploadBlock(blockId, srcStream, (long)bytesToRead);
                blob.uploadBlock(blockId, srcStream, (long)bytesToRead, null, blobReqOpts, null);

                /* increment/decrement counters */         
                bytesLeft -= bytesToRead;
            }/* end of while*/
	        srcStream.close();
			//System.out.println(blockList.toString());
	        blob.commitBlockList(blockList);
			totalTime = System.currentTimeMillis() - startTime ;
			
			/* refresh the properties */
			blob.downloadAttributes();
			logger.trace("Upload completed, from {} to {} , CommitedBLocks: {}, Bytes Uploaded: {}. The time spent is {} ms. ",srcLocalFileFullPath, 
					reqParams.getBlobFullPath() ,blockList.size(), blob.getProperties().getLength(), totalTime);
		} catch (Exception  ex) {
			 String errMessage = "Exception occurred when upload the file:" + srcLocalFileFullPath + " to " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			 BfsUtility.throwBlobfsException(ex, errMessage);
		 } finally {
			 if (null != srcStream){
				 try{
		        	srcStream.close();
			     }catch (Exception ex){
			    	 String errMessage = "Exception occurred when close the file:" + srcLocalFileFullPath + ". " + ex.getMessage();
			    	 BfsUtility.throwBlobfsException(ex, errMessage);

			     }
			  }
		 }
	}
	
	/* create the blob */	
	/**
	 * @param reqParams, container, blob, blobType, doForce
	 * 		  only support BLOCK, APPEND, PAGE blob
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean createBlob(BlobReqParams reqParams) throws BlobfsException {
		boolean result = false;
		AccessCondition accCondtion = new AccessCondition();
		CloudBlob blob = getBlobReference(reqParams);
		try {
			if (blob.exists() && !reqParams.isDoForoce()){
				String errMessage = "The target blob: " + reqParams.getBlobFullPath() + " already exists.";
				throw new BlobfsException(errMessage);
			}
			/* set the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				accCondtion.setLeaseID(reqParams.getLeaseID());
			}
			/* create a empty temporary file */
	        File tempFile = File.createTempFile(Constants.BLOBFS_TEMP_FILE_PREFIX, "tmp");
	        FileInputStream fileInputStream = null;
			/* if it's page blob, we should initialize the blob */
			if ("PAGEBLOB".equals(reqParams.getBfsBlobType().toString())){
				/* fill the temp file with random data */
				byte[] zeroBytes = new byte[Constants.PAGEBLOB_MINIMUM_SIZE];
				FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
				fileOutputStream.write(zeroBytes);
				fileOutputStream.close();
				/* initialize the page blob */
		        long blobSize = (null == reqParams.getBlobSize()) ? tempFile.length() : reqParams.getBlobSize();
				((CloudPageBlob) blob).create(blobSize);
				/* upload the data */
				fileInputStream = new FileInputStream(tempFile);
				((CloudPageBlob) blob).uploadPages(fileInputStream, 0, Constants.PAGEBLOB_MINIMUM_SIZE, accCondtion, null, null);

			}else{
				int length = 0;
				if (null != reqParams.getContent()){
					byte[] contentBytes = reqParams.getContent().getBytes(Constants.DEFAULT_CHARSET);
					length = contentBytes.length;
					FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
					fileOutputStream.write(contentBytes);
					fileOutputStream.close();	
				}
				fileInputStream = new FileInputStream(tempFile);
				blob.upload(fileInputStream, length, accCondtion, null, null);
			}			
			/*  Delete tmp file when upload success. */
			if (null != fileInputStream) {fileInputStream.close();}
			tempFile.deleteOnExit();
			result = true;
			logger.trace("the blob：{} has been created. ", reqParams.getBlob());

		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when creating the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		} 
		return result;
	}	
	/* delete the blob */	
	/**
	 * @param reqParams, container, blob, blobType, doForce
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean deleteBlob(BlobReqParams reqParams) throws BlobfsException {
		boolean result = false;
		AccessCondition accCondtion = new AccessCondition();
		CloudBlob blob = getBlobReference(reqParams);		
		try {
			/* set the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				accCondtion.setLeaseID(reqParams.getLeaseID());
			} else if (blob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
				reqParams.setBlobInstance(blob);
				String leaseID = getBlobMetadata(reqParams, Constants.BLOB_META_DATA_LEASE_ID_KEY);
				if (null != leaseID ) { accCondtion.setLeaseID(leaseID); }
			}
			blob.deleteIfExists(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, accCondtion, null, null);
			result = true;
			logger.trace("the blob：{} has been deleted. ", reqParams.getBlob());

		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when deleting the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	/**
	 * check if the blob exists
	 * @param reqParams
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean blobExists(BlobReqParams reqParams) throws BlobfsException {
		boolean result = false;
		/* for checking whether the blob exists, the blob type can be used blindly */
		BfsBlobType bfsBlobType = (null == reqParams.getBfsBlobType()) 
									? BfsBlobType.BLOCKBLOB :reqParams.getBfsBlobType();
		reqParams.setBfsBlobType(bfsBlobType);
		CloudBlob blob = getBlobReference(reqParams);		
		try {
			if (blob.exists()){
				result = true;
			}
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when checking the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	/*upload the append blob */	
	/**
	 * @param reqParams: localDir, LocalFile, container, blob, doForoce
	 * @return
	 * @throws BlobfsException
	 */
	public final static void uploadAppendBlobWithSinglePart (BlobReqParams reqParams) throws BlobfsException {
		String srcLocalFileFullPath = reqParams.getLocalFileFullPath();
		/*  when create the append blob firstly, the blob reference pointer for getProperties().getAppendBlobCommittedBlockCount
		 *  is null, so use this variable to fix the bug */
		int committedBlockCount = 0;
		FileInputStream srcStream = null;
		long startTime = 0;
		try {
			/* check the source file */
			File srcFile = new File(srcLocalFileFullPath);
			if (!srcFile.exists()){
			    String errMessage = "The source file:" + srcLocalFileFullPath + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			/* check the block size of source file */
			if ((long)srcFile.length() > Constants.APPENDBLOB_BLOCK_SIZE_LIMIT){ //4MB
				String errMessage = "The source file: " + srcLocalFileFullPath + " exceeds the 4MB size limit of one append block.";
				throw new BlobfsException(errMessage);
			}
			srcStream = new FileInputStream(srcFile);
			reqParams.setBfsBlobType(BfsBlobType.APPENDBLOB);
			CloudAppendBlob blob = (CloudAppendBlob) getBlobReference(reqParams);
			/* create if it does not exist */
			if (!blob.exists()){
				blob.createOrReplace();
				logger.trace("Created the append blob: {} ",reqParams.getBlobFullPath());
			}
			/* check the block count of the append blob */
			committedBlockCount = (null != blob.getProperties().getAppendBlobCommittedBlockCount()) ? blob.getProperties().getAppendBlobCommittedBlockCount() : 0;
			if (committedBlockCount > Constants.BLOB_BLOCK_NUMBER_LIMIT){ //50000
				String errMessage = "The block count of target blob: " + reqParams.getBlobFullPath() + " exceeds the 50,000 count limit.";
				throw new BlobfsException(errMessage);
			}
			/* check the size append blob */
			if ((long)blob.getProperties().getLength() + (long)srcFile.length() > Constants.APPENDBLOB_SIZE_LIMIT){ //195GB
			    String errMessage = "The size of target blob: " + reqParams.getBlobFullPath() + " exceeds the 195GB size limit.";
				throw new BlobfsException(errMessage);
			}
			/* set the request options */
			BlobRequestOptions blobReqOpts = new BlobRequestOptions();
			startTime = System.currentTimeMillis();
			logger.trace("Start uploading: {}, the file size is {} ", reqParams.getBlobFullPath(), srcFile.length());
			/* per block is limit to 4MB, if we split the big file into 4MB blocks automatically
			    it may split a line midway and it matters for csv file */
			blob.append(srcStream, srcFile.length(), null, blobReqOpts, null);
			//blob.uploadFromFile(sourcePath, accessCondition, blobReqOpts, null);
			srcStream.close();
			long totalTime = System.currentTimeMillis() - startTime;
			/* refresh the properties */
			blob.downloadAttributes();
			logger.trace("Upload completed: {}, the total size of the append blob is {}, The time spent is {} ms. ", reqParams.getBlobFullPath(), blob.getProperties().getLength(), totalTime);

		} catch (Exception  ex) {
			 String errMessage = "Exception occurred when upload the file:" + srcLocalFileFullPath + " to " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			 BfsUtility.throwBlobfsException(ex, errMessage);
		 } finally {
			 if (null != srcStream){
				 try{
		        	srcStream.close();
			     }catch (Exception ex){
			    	 String errMessage = "Exception occurred when close the file:" + srcLocalFileFullPath + ". " + ex.getMessage();
			    	 BfsUtility.throwBlobfsException(ex, errMessage);	

			     }
			  }
		 }
		return;
														
	}
	/*upload the append blob, if the size is bigger than 4MB, we will split it */
	/**
	 * @param reqParams: localDir, localFile, tmpDir, container, blob
	 * @return
	 * @throws BlobfsException
	 */
	public final static void uploadAppendBlobWithMultiParts (BlobReqParams reqParams) throws BlobfsException {
		String srcLocalDir = reqParams.getLocalDir();
		String srcLocalFile = reqParams.getLocalFile();
		String srcLocalFileFullPath = reqParams.getLocalFileFullPath();
		String tmpDir = (null == reqParams.getLocalTmpDir()) ? System.getProperty("java.io.tmpdir") : reqParams.getLocalTmpDir();

		/*  when create the append blob firstly, the blob reference pointer for getProperties().getAppendBlobCommittedBlockCount
		 *  is null, so use this variable to fix the bug */
		int committedBlockCount = 0;
        int chunkSize = Constants.APPENDBLOB_SPLIT_CHUNK_SIZE;
		long startTime = 0;
		ArrayList<String> fileChunksList = null;
		int numberOfChunks = 0;
		int chunkCounter = 0;
		try {
			/* check the source file */
			File srcFile = new File(srcLocalFileFullPath);
			if (!srcFile.exists()){
				String errMessage = "The source file:" + srcLocalFileFullPath + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			long srcFileLength = srcFile.length();
			/* check the file size */
			if (srcFileLength <= chunkSize){
				uploadAppendBlobWithSinglePart(reqParams);
				return;
			}
			/* split the source file */
			if ((fileChunksList = BfsUtility.splitFileIntoChunks(srcLocalDir, srcLocalFile, chunkSize, tmpDir)) == null){
			    String errMessage = "Unexpected exception occurred when spliting the  file "+ srcLocalFile +".";
				throw new BlobfsException(errMessage);
			}
			reqParams.setBfsBlobType(BfsBlobType.APPENDBLOB);
			CloudAppendBlob blob = (CloudAppendBlob) getBlobReference(reqParams);
			/* create if it does not exist */
			if (!blob.exists()){
				blob.createOrReplace();
				logger.trace("Created the append blob: {} ",reqParams.getBlobFullPath());
			}
            numberOfChunks  = fileChunksList.size();
			/* check the block count of the append blob */
			committedBlockCount = (null != blob.getProperties().getAppendBlobCommittedBlockCount()) ? blob.getProperties().getAppendBlobCommittedBlockCount() : 0;
			if (committedBlockCount + numberOfChunks > Constants.BLOB_BLOCK_NUMBER_LIMIT){ //50000
				String errMessage = "The block count of target blob: " + reqParams.getBlobFullPath() + " exceeds the 50,000 count limit.";
				throw new BlobfsException(errMessage);
			}
			/* check the size append blob */
			if ((long)blob.getProperties().getLength() + srcFileLength > Constants.APPENDBLOB_SIZE_LIMIT){ //195GB
			    String errMessage = "The size of target blob: " + reqParams.getBlobFullPath() + " exceeds the 195GB size limit.";
				throw new BlobfsException(errMessage);
			}
			/* set the request options */
			BlobRequestOptions blobReqOpts = new BlobRequestOptions();
			startTime = System.currentTimeMillis();
			logger.trace("Start uploading, from {} to {}, the file size is {} ", srcLocalFile, srcLocalFileFullPath, srcFileLength);
			/* per block is limit to 4MB, if we split the big file into 4MB blocks automatically
			    it may split a line midway and it matters for csv file */
			for (int i = 0; i < numberOfChunks; i++) {
				 chunkCounter ++;
				 String fileNamePerChunk = fileChunksList.get(i);
                 /* upload the chunk to the blob */
				 //System.out.println("fileNamePerChunk:" + fileNamePerChunk);
	             blob.appendFromFile(fileNamePerChunk, null, blobReqOpts, null);
                 /*  trace out progress */
                 float percentageDone = ((float)chunkCounter / (float)numberOfChunks) * (float)100;
				 logger.trace("The source file: {} , blockId: {}. {}% done", srcLocalFileFullPath, chunkCounter, percentageDone);                  
             }				
			long totalTime = System.currentTimeMillis() - startTime;
			/* refresh the properties */
			blob.downloadAttributes();
			logger.trace("Upload completed: from {} to {}, the total size of the append blob is {}, The time spent is {} ms. ", srcLocalFile, reqParams.getBlobFullPath(), blob.getProperties().getLength(), totalTime);
		} catch (Exception  ex) {
			 String errMessage = "Unexpected exception occurred appending the file: " + srcLocalFile +" to the append blob: "+ reqParams.getBlobFullPath() +". " + ex.getMessage();
			 BfsUtility.throwBlobfsException(ex, errMessage);
		} finally{
			/* clean the temporary files */
			if (null != fileChunksList){
				for(String tmpFilePerChunk : fileChunksList){
					try {
						Files.deleteIfExists(Paths.get(tmpFilePerChunk));
					} catch (IOException ex) {
						 String errMessage = "Unexpected exception occurred when deleting the temporay file " + tmpFilePerChunk + ". " + ex.getMessage();
						 BfsUtility.throwBlobfsException(ex, errMessage);
					}
				}
			}
		}
	}
	/* move or rename the append blob */
	/**
	 * @param reqParams: optMode, blobType, container, blob, destContainer, destBlob, doForce
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean CopyOrmoveBlobCrossContainer (BlobReqParams reqParams) throws BlobfsException {
		boolean result = false;
		try {
			/* move or copy the virtual directory, default is copy */
			BlobOptMode optMode = (null == reqParams.getBlobOptMode()) ? BlobOptMode.COPY : reqParams.getBlobOptMode();
			String sourceContainer = reqParams.getContainer();
			String sourePath = reqParams.getBlob();
			String destContainer = reqParams.getDestContainer();
			String destPath = reqParams.getDestBlob();
			boolean doForce = reqParams.isDoForoce();
			/* set the default type */
			BfsBlobType bfsBlobType;
			String leaseID = null;
			/* get the blob type, if the caller is CopyOrmoveVirtualDirectory, don't need the check type and existing  */
			if (null == (bfsBlobType = reqParams.getBfsBlobType())){
				if (null == (bfsBlobType = getBlobType(reqParams))){
					String errMessage = "The source blob: " + reqParams.getBlobFullPath() + " does not exist.";
					throw new BlobfsException(errMessage);
				}
			}
			/* the interval for checking the coping status */
			int getCSinterval = 0;
			/* leaseTimeInSeconds should be between 15 and 60 */
			int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
			AccessCondition accCondtion = null;
			CloudBlob sourceBlob = null;
			CloudBlob destBlob = null;
			long startTime = 0;
			
			/* check the source path and dest path, if they are the same, azure will throw lease exception */
			if (sourceContainer.equals(destContainer) && sourePath.equals(destPath)){
				return true;
			}
			sourceBlob = getBlobReference(reqParams);
			BlobReqParams destReq = new BlobReqParams();
			destReq.setBfsBlobType(bfsBlobType);
			destReq.setContainer(destContainer);
			destReq.setBlob(destPath);
			destBlob = getBlobReference(destReq);

//			if (destBlob.exists() && !doForce){
//				String errMessage = "The target blob: " + reqParams.getDestBlobFullPath() + " already exists.";
//				throw new BlobfsException(errMessage);	
//			}
			/* lock the source blob */				 
			leaseID = sourceBlob.acquireLease(minLockedSec, null);
			/* start the move */
			String copyJobId = destBlob.startCopy(sourceBlob.getUri());
			startTime = System.currentTimeMillis();
			logger.trace("Start copying, from {} to {} ,Copy ID is {}, The size is {}.", reqParams.getBlobFullPath(), 
						reqParams.getDestBlobFullPath() ,copyJobId, sourceBlob.getProperties().getLength());
			CopyState copyState = destBlob.getCopyState();
			accCondtion = new AccessCondition();
			accCondtion.setLeaseID(leaseID);
            while (copyState.getStatus().equals(CopyStatus.PENDING)) {
            	getCSinterval ++ ;
            	Thread.sleep(50);
            	/* keep locking the object */
            	if (getCSinterval >= (int) 20 * minLockedSec){
            		getCSinterval = 0;
            		sourceBlob.renewLease(accCondtion);
            	}
            }                
			/* free the source blob */
			//if (sourceBlob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
				sourceBlob.releaseLease(accCondtion);
			//}
			/* if the operation mode is move, we should delete the source file */
			if ("MOVE".equals(optMode.toString())){
				/* delete the source blob */
				sourceBlob.deleteIfExists(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
			}
			result = true;
			long totalTime = System.currentTimeMillis() - startTime;
			/* refresh the properties */
			destBlob.downloadAttributes();
			logger.trace("Copy completed, from {} to {} ,Copy ID is {}, The size is {}, The time spent is {} ms.",reqParams.getBlobFullPath(), 
					reqParams.getDestBlobFullPath() ,copyJobId, destBlob.getProperties().getLength(), totalTime);
		
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when moving the blob from " 
		    					+reqParams.getBlobFullPath() + " to " + reqParams.getDestBlobFullPath() + ". " + ex.getMessage() ;
		    BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/*download the blob with single part */
	/**
	 * @param reqParams blobType， container, blob, localDir, localFile, doForce
	 * @throws BlobfsException
	 */
	public final static void downloadBlobWithSinglePart (BlobReqParams reqParams) throws BlobfsException {
		String destLocalFileFullPath = reqParams.getLocalFileFullPath();
		boolean doForce = reqParams.isDoForoce();
		CloudBlob  blob = null;
		/* if don't lock the blob ,* will produce the corrupted file if the source blob is updated or deleted during the downloading*/
		/* so we should check the blob whether is locked by other session, if so we need refresh the lease during carefully*/
		//String leaseID = null;
		/* leaseTimeInSeconds should be between 15 and 60 */
		//int minLockedSec = 15;
		//AccessCondition accCondtion = null;
		long startTime = 0;
		long totalTime = 0;
		/* check the local file */
		File destLocalFile = new File(destLocalFileFullPath);
		if (destLocalFile.isFile()){
			if (!doForce){
				String errMessage = "The target local file: " + destLocalFileFullPath + " already exists.";
				throw new BlobfsException(errMessage);
			}
			/* else we should remove the file first  */
			destLocalFile.delete();
		}
		blob = getBlobReference(reqParams);
		try {
			/* check the source blob */
			if (!blob.exists()){
				String errMessage = "The source blob: " + reqParams.getBlobFullPath() + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			long blobSizeToDL = 0;
			boolean isBlobResize = false;
			/* used for resize the blob */
			if (null != reqParams.getBlobSize() && blob.getProperties().getLength() > reqParams.getBlobSize()){
				blobSizeToDL = reqParams.getBlobSize();
				isBlobResize = true;
			}else{
				blobSizeToDL = blob.getProperties().getLength();
			}
			/* lock the source blob */				 				 
			//leaseID = blob.acquireLease(minLockedSec, null);
			/* start the downloading */
			logger.trace("Start downloading, from {} to {} ,The size is {}. ",reqParams.getBlobFullPath(), 
					destLocalFileFullPath, blobSizeToDL);
               
			startTime = System.currentTimeMillis();
			// Set logging off by default.  
			OperationContext ctx = new OperationContext();  
			//ctx.setLoggingEnabled(true);	        
			//blob.download(new BufferedOutputStream(new FileOutputStream(destLocalPath)));
			if (isBlobResize){
				blob.downloadRange(0, blobSizeToDL, new BufferedOutputStream(new FileOutputStream(destLocalFileFullPath)), null, null, ctx);
			}else{
				blob.download(new BufferedOutputStream(new FileOutputStream(destLocalFileFullPath)), null, null, ctx);
			}
            totalTime = System.currentTimeMillis() - startTime ;
			logger.trace("Download completed, from {} to {} , Bytes downloade: {}. The time spent is {} ms. ", reqParams.getBlobFullPath(), 
					destLocalFileFullPath ,destLocalFile.length(), totalTime);
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when downloading the blob from " 
		    				+ reqParams.getBlobFullPath() + " to " + destLocalFileFullPath + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);
		}
	}
	/*download the blob with multiparts */
	/**
	 * @param reqParams blobType， container, blob, localDir, localFile, doForce
	 * @throws BlobfsException
	 */
	public final static void downloadBlobWithMultiParts (BlobReqParams reqParams) throws BlobfsException {
		String destLocalFileFullPath = reqParams.getLocalFileFullPath();
		boolean doForce = reqParams.isDoForoce();
		CloudBlob  blob = null;
		/* if don't lock the blob ,* will produce the corrupted file if the source blob is updated or deleted during the downloading*/
		/* so we should check the blob whether is locked by other session, if so we need refresh the lease during carefully*/
		//String leaseID = null;
		/* leaseTimeInSeconds should be between 15 and 60 */
		//int minLockedSec = 15;
		//AccessCondition accCondtion = null;
		long startTime = 0;
		long totalTime = 0;
		/* check the local file */
		File destLocalFile = new File(destLocalFileFullPath);
		if (destLocalFile.isFile()){
			if (!doForce){
				String errMessage = "The target local file: " + destLocalFileFullPath + " already exists.";
				throw new BlobfsException(errMessage);
			}
			/* else we should remove the file first  */
			destLocalFile.delete();
		}		
		blob = getBlobReference(reqParams);
		try {
			/* check the source blob */
			if (!blob.exists()){
				String errMessage = "The source blob: " + reqParams.getBlobFullPath() + " does not exist.";
				throw new BlobfsException(errMessage);
			}
			/* set the count */
			long srcBlobSize = 0;
			/* used for resize the blob */
			if (null != reqParams.getBlobSize() && (long)blob.getProperties().getLength() > reqParams.getBlobSize()){
				srcBlobSize = reqParams.getBlobSize();

			}else{
				srcBlobSize = (long)blob.getProperties().getLength();
			}
            int blockSize = Constants.DOWNLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE; // 4MB
            int blockCount = (int)((float)srcBlobSize / (float)blockSize) + 1;
            long bytesLeft = srcBlobSize;
            int blockNumber = 0;
            long bytesRead = 0;
			/* lock the source blob */				 				 
			//leaseID = blob.acquireLease(minLockedSec, null);
			/* start the downloading */
            logger.trace("Start downloading, from {} to {} ,The size is {}. ",reqParams.getBlobFullPath(), 
					destLocalFileFullPath, srcBlobSize);
            startTime = System.currentTimeMillis();
            while( bytesLeft > 0 ) {	            	
                blockNumber++;
                /* how much to read (only last chunk may be smaller) */
                int bytesToRead = 0;
                if ( bytesLeft >= (long)blockSize ) {
                  bytesToRead = blockSize;
                } else {
                  bytesToRead = (int)bytesLeft;
                }
                /* trace out progress */
                float percentageDone = ((float)blockNumber / (float)blockCount) * (float)100;
                /* download block chunk to local disk */
				blob.downloadRange(bytesRead, (long)bytesToRead, new BufferedOutputStream(new FileOutputStream(destLocalFileFullPath,true)));
				logger.trace("Start downloading, from {} to {} ,The block size is {}, blockId: {}. {}% done. ",reqParams.getBlobFullPath(), 
						destLocalFileFullPath, bytesToRead, blockNumber, percentageDone);
                /* increment/decrement counters */         
                bytesRead += bytesToRead;
                bytesLeft -= bytesToRead;
            }/* end of while*/
			totalTime = System.currentTimeMillis() - startTime ;
			logger.trace("Download completed, from {} to {} , Bytes downloaded: {}. The time spent is {} ms. ",reqParams.getBlobFullPath(), 
					destLocalFileFullPath ,destLocalFile.length(), totalTime);
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when downloading the blob from " 
		    				+ reqParams.getBlobFullPath() + " to " + destLocalFileFullPath + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);
		}
	}
	/* set the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param key
	 * @param value
	 * @throws BlobfsException
	 */
	public final static void setBlobMetadata (BlobReqParams reqParams, String key, String value) throws BlobfsException {
		CloudBlob blob = null;
		String leaseID = null;
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			leaseID = (null != reqParams.getLeaseID()) ? reqParams.getLeaseID() : null;
			AccessCondition accCondtion = new AccessCondition();
			if (null != leaseID){
				accCondtion.setLeaseID(leaseID);
			}
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (null == metadata) {
		      metadata = new HashMap<String, String>();
		    }
		    metadata.put(key, value);
		    blob.setMetadata(metadata);
		    /* upload the meta data to blob service */
		    blob.uploadMetadata(accCondtion, null, null);
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". key:" + key + " . value: " +value + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		
	}
	/* get the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param keyAlternatives
	 * @return
	 * @throws BlobfsException
	 */
	public final static String getBlobMetadata (BlobReqParams reqParams, String... keyAlternatives) throws BlobfsException {
		CloudBlob blob = null;
		reqParams.setBfsBlobType(null);
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			/* down load the meta data firstly */
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (null == metadata) {
		      return null;
		    }
		    for (String key : keyAlternatives) {
		      if (metadata.containsKey(key)) {
		        return metadata.get(key);
		      }
		    }
		} catch (StorageException ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". keys: " + keyAlternatives.toString() +  ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	    return null;
	}
	/* remove the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param keyAlternatives
	 * @return
	 * @throws BlobfsException
	 */
	public final static void removeBlobMetadata (BlobReqParams reqParams, String key) throws BlobfsException {
		CloudBlob blob = null;
		String leaseID = null;
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			leaseID = (null != reqParams.getLeaseID()) ? reqParams.getLeaseID() : null;
			AccessCondition accCondtion = new AccessCondition();
			if (null != leaseID && blob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
					accCondtion.setLeaseID(leaseID);
			}
			/* down load the meta data firstly */
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (metadata != null) {
		    	 if (metadata.containsKey(key)) {
			        metadata.remove(key);
			     }
			     blob.setMetadata(metadata);
			     /* upload the meta data to blob service */
			     blob.uploadMetadata(accCondtion, null, null);
		    }		 
		} catch (StorageException ex) {
			String errMessage = "Unexpected exception occurred when remove metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". key: " + key + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	}
	/* Getting the properties of the blob */
	/**
	 * @param reqParams: blobType，container, blob
	 * @throws BlobfsException
	 */
	public final static BlobProperties getBlobProperties (BlobReqParams reqParams) throws BlobfsException {
		BlobProperties blobPorperties = new BlobProperties();
		CloudBlob blob = null;
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			blob.downloadAttributes();
			blobPorperties.setName(blob.getName());
			blobPorperties.setBfsBlobType(blob.getProperties().getBlobType());
			blobPorperties.setContentMD5(blob.getProperties().getContentMD5());
			blobPorperties.setEtag(blob.getProperties().getEtag());
			blobPorperties.setLeaseDuration(blob.getProperties().getLeaseDuration());
			blobPorperties.setLeaseState(blob.getProperties().getLeaseState());
			blobPorperties.setCreated(blob.getProperties().getLastModified());
			blobPorperties.setLastModified(blob.getProperties().getLastModified());
			blobPorperties.setLength(blob.getProperties().getLength());	
			blobPorperties.setActualLength(blob.getProperties().getLength());
			/* fill the page blob */
			if ("PAGEBLOB".equals(blobPorperties.getBfsBlobType().toString())){
				blobPorperties.setActualLength(getPageBlobActualLength(reqParams));
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BlobfsException(errMessage);
		}		
		return blobPorperties;	
	}
	/* check the directory exists or not */
	/**
	 * @param reqParams: container, blob
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean virtualDirectoryExists (BlobReqParams reqParams) throws BlobfsException {
		boolean result  = false;
		try {
			CloudBlobContainer container = ContainerService.getPrivateContainer(reqParams.getContainer());
			/* count() is not available up to this 5.0.0 */
			/* should check the last char, should add slash */
			String virtualDir = (reqParams.getBlob().endsWith("/")) ? reqParams.getBlob() : reqParams.getBlob() + "/";
			Iterable<ListBlobItem> collection = container.listBlobs(virtualDir, true);	
			if (null != collection){
				for (ListBlobItem blobItem : collection) {
					if (blobItem instanceof CloudBlob) {
						/* make sure at least one blob within the directory */
						result = true;
						break;
					}
				}
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when checking the virtual directory : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);

		}		
		return result;
	}
	
	/* get the blob type */
	/**
	 * @param reqParams: container, blob
	 * @return
	 * @throws BlobfsException
	 */
	public final static BfsBlobType getBlobType (BlobReqParams reqParams) throws BlobfsException {
		BfsBlobType bfsBlobType = null;
		try {
			bfsBlobType = getBlobProperties(reqParams).getBfsBlobType();
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the blob type : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);

		}		
		return bfsBlobType;
	}
	/* change block blob to append blob */
	/**
	 * @param reqParams:blobType, container, blob, localTmpDir
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean changeBlocBlobToAppendBlob (BlobReqParams reqParams) throws BlobfsException {
		String sourceBlob = reqParams.getBlob();
		String leaseID = null;
		int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
		AccessCondition accCondtion = new AccessCondition();
        String randomString = Long.toHexString(Double.doubleToLongBits(Math.random()));
		String appendBlobTempName = sourceBlob + "-" + randomString + ".tmp";
		boolean result = false;		
		/* check the blob type */
		if ("APPENDBLOB".equals(getBlobType(reqParams).toString())){
			logger.trace("The blob: {} is the append blob already!", sourceBlob);
			return true;
		}
		try {
			logger.trace("Start changing the type of the blob: {}. from block to append", sourceBlob);
			CloudBlob blob = getBlobReference(reqParams);
			blob.downloadAttributes();
			long srcBlobSize = blob.getProperties().getLength();
			/* create the temporary blob */
			BlobReqParams outsReq = new BlobReqParams();
			outsReq.setContainer(reqParams.getContainer());
			outsReq.setBlob(appendBlobTempName);
			outsReq.setBfsBlobType(BfsBlobType.APPENDBLOB);
			outsReq.setDoForoce(true);
			createBlob(outsReq);
			/* start to transfer the data */
			if (srcBlobSize > 0){				
				/* create the output stream*
				 * read data from source blob */
				BlobReqParams insReq = new BlobReqParams();
				insReq.setContainer(reqParams.getContainer());
				insReq.setBlob(reqParams.getBlob());
				insReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
				BlobBufferedIns bbIns = new BlobBufferedIns(insReq);
				/* create the output stream
				 * write data the to temporary blob */
				BlobBufferedOus bbOus =  new BlobBufferedOus(outsReq);
				/* lock the blob */
				leaseID = blob.acquireLease(minLockedSec, null);
				accCondtion.setLeaseID(leaseID);
				/* set counters */
				int blockSize = Constants.UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE;
				long bytesLeft = srcBlobSize;
				/* loop transfer the date from the source blob to target blob */
				while( bytesLeft > 0 ) {
					/* how much to read (only last chunk may be smaller) */
					int bytesToRead = 0;
					if ( bytesLeft >= (long)blockSize ) {
						bytesToRead = blockSize;
					} else {
						bytesToRead = (int)bytesLeft;
					}
					byte[] bytesReaded = new byte[bytesToRead];
					if (bbIns.read(bytesReaded, (int)bbIns.readOffset, bytesToRead) != -1){
						bbOus.write(bytesReaded, 0, bytesToRead);
					}   
					/* increment/decrement counters */         
					bytesLeft -= bytesToRead;
					/* renew the lease */
					blob.renewLease(accCondtion);
				}/* end of while*/
				bbIns.close();           
				/* close the output stream */
				bbOus.close();			
			}
			
			/* due the new blob and original blob are different type, so should delete the original blob first */
			/* if the delete operation failed, we should keep the temporary append blob as backup and return false */
			BlobReqParams delReq = new BlobReqParams();
			delReq.setBlob(sourceBlob);
			delReq.setContainer(reqParams.getContainer());
			delReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			if (srcBlobSize > 0) { delReq.setLeaseID(leaseID);}
			deleteBlob(delReq);

			/* rename temporary file name to the original blob name */
			BlobReqParams renameReq = new BlobReqParams();
			renameReq.setBlobOptMode(BlobOptMode.MOVE);
			renameReq.setContainer(reqParams.getContainer());
			renameReq.setBlob(appendBlobTempName);
			renameReq.setDestBlob(sourceBlob);
			renameReq.setDestContainer(reqParams.getContainer());
			renameReq.setDoForoce(true);
			renameReq.setBfsBlobType(BfsBlobType.APPENDBLOB);
			CopyOrmoveBlobCrossContainer(renameReq);
			result = true;
			logger.trace("The block blob: {} is changed to the append blob successfully.", sourceBlob);
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when changing the blob type : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		} finally {
			/* clean the local temporary file  */			
			System.gc();
		}		
		return result;
	}
	/* change the blob size */
	/**
	 * @param reqParams:blobType, container, blob, localTmpDir
	 * @return
	 * @throws BlobfsException
	 */
	public final static boolean changeBlobSize (BlobReqParams reqParams) throws BlobfsException {
		String sourceBlob = reqParams.getBlob();
		long tgtBlobSize = reqParams.getBlobSize();
		int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
		String leaseID;
		BfsBlobType blobType = null;
        String randomString = Long.toHexString(Double.doubleToLongBits(Math.random()));
		String blobTempName = sourceBlob + "-" + randomString + ".tmp";
		boolean result = false;
		try {
			CloudBlob blob = getBlobReference(reqParams);
			blob.downloadAttributes();
			long srcBlobSize = blob.getProperties().getLength();
			/* the size of source blob is zero */
			if (srcBlobSize == 0){ return true; }
			/* get the blob type */
			if (null == reqParams.getBfsBlobType()){
				blobType = getBlobType(reqParams);
				reqParams.setBfsBlobType(blobType);
			}
			logger.trace("Start resizing the the blob: {} from {} to {}.", sourceBlob, srcBlobSize, tgtBlobSize);
			/* get the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				leaseID = getBlobMetadata(reqParams, Constants.BLOB_META_DATA_LEASE_ID_KEY);
			} else {				
				leaseID = blob.acquireLease(minLockedSec, null);
			}
			/* resize to zero, overwrite the original blob with with zero length */
			if (tgtBlobSize == 0){
				reqParams.setDoForoce(true);
				reqParams.setLeaseID(leaseID);
				reqParams.setBfsBlobType(blobType);
				createBlob(reqParams);
				logger.trace("The size of the blob: {} is changed to {} from {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
				return true;
			}
			/* resize the page blob */
			if ("PAGEBLOB".equals(reqParams.getBfsBlobType().toString())){
				return changePageBlobSize(reqParams);
			}
			/* read data from source blob */
			BlobReqParams insReq = new BlobReqParams();
			insReq.setContainer(reqParams.getContainer());
			insReq.setBlob(reqParams.getBlob());
			insReq.setBfsBlobType(reqParams.getBfsBlobType());
			BlobBufferedIns bbIns = new BlobBufferedIns(insReq);
			
			/* write data to the temporary blob */
			BlobReqParams outsReq = new BlobReqParams();
			outsReq.setContainer(reqParams.getContainer());
			outsReq.setBlob(blobTempName);
			outsReq.setBfsBlobType(reqParams.getBfsBlobType());
			outsReq.setDoForoce(true);
			createBlob(outsReq);
			BlobBufferedOus bbOus =  new BlobBufferedOus(outsReq);
			/* we need fill the target blob with zero bytes when extending source blob */
			long bytesToFillWithZero = (int) Math.max(0, tgtBlobSize - srcBlobSize);
			/* start to transfer the data */
			/* set counters */
			long bytesFromSrcBlob = Math.min(tgtBlobSize, srcBlobSize);
            int blockSize = Constants.UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE;
            long bytesLeft = bytesFromSrcBlob;
            /* loop transfer the date from the source blob to target blob */
            while( bytesLeft > 0 ) {
                 /* how much to read (only last chunk may be smaller) */
                int bytesToRead = 0;
                if ( bytesLeft >= (long)blockSize ) {
                	bytesToRead = blockSize;
                } else {
                	bytesToRead = (int)bytesLeft;
                }
                byte[] bytesReaded = new byte[bytesToRead];
              	if (bbIns.read(bytesReaded, (int)bbIns.readOffset, bytesToRead) != -1){
              		bbOus.write(bytesReaded, 0, bytesToRead);
                }   
                /* increment/decrement counters */         
                bytesLeft -= bytesToRead;
            }/* end of while*/
            bbIns.close();
            
            /* expand the blob with zero bytes */
            if (bytesToFillWithZero > 0){
                 bytesLeft = bytesToFillWithZero;
	             while( bytesLeft > 0 ) {	            	
	                 /* how much to read (only last chunk may be smaller) */
	                 int bytesToRead = 0;
	                 if ( bytesLeft >= (long)blockSize ) {
	                  	bytesToRead = blockSize;
	                 } else {
	                  	bytesToRead = (int)bytesLeft;
	                 }
	                 byte[] bytesReaded = new byte[bytesToRead];
	          		 bbOus.write(bytesReaded, 0, bytesToRead);  
	                 /* increment/decrement counters */         
	                 bytesLeft -= bytesToRead;
	            }
            }
            /* must close the output stream, this will commit the uploaded data to storage service */
            bbOus.close();
			/* in order to keep the file safe we should upload the temp file, and then renmae */
			/* if the delete operation failed, we should keep the temporary append blob as backup and return false */
			BlobReqParams delReq = new BlobReqParams();
			delReq.setBlob(sourceBlob);
			delReq.setContainer(reqParams.getContainer());
			delReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			delReq.setLeaseID(leaseID);
			deleteBlob(delReq);

			/* rename temporary file name to the original blob name */
			BlobReqParams renameReq = new BlobReqParams();
			renameReq.setBlobOptMode(BlobOptMode.MOVE);
			renameReq.setContainer(reqParams.getContainer());
			renameReq.setBlob(blobTempName);
			renameReq.setDestBlob(sourceBlob);
			renameReq.setDestContainer(reqParams.getContainer());
			renameReq.setDoForoce(true);
			renameReq.setBfsBlobType(reqParams.getBfsBlobType());
			CopyOrmoveBlobCrossContainer(renameReq);

			result = true;
			logger.trace("The size of the blob: {} is changed from {} to {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when resizing the blob: " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		} finally {
			/* clean the temporary files */
			System.gc();
		}		
		return result;
	}
	
	/* change the blob size */
	public final static boolean changePageBlobSize (BlobReqParams reqParams) throws BlobfsException {
		boolean result = false;
		String sourceBlob = reqParams.getBlob();
		long tgtBlobSize = reqParams.getBlobSize();
		long finalBlobSize = Constants.PAGEBLOB_MINIMUM_SIZE;
		long srcBlobSize = -1;
		try {
			reqParams.setBfsBlobType(BfsBlobType.PAGEBLOB);
			CloudPageBlob blob = (CloudPageBlob)getBlobReference(reqParams);
			if (blob.exists()){
				BlobReqParams getSizeReq = new BlobReqParams();
				getSizeReq.setContainer(reqParams.getContainer());
				getSizeReq.setBlob(reqParams.getBlob());
				getSizeReq.setBfsBlobType(reqParams.getBfsBlobType());
				long actualPageBlobSize = getPageBlobActualLength(getSizeReq);
				if (actualPageBlobSize >= tgtBlobSize){ // shrink the blob
					finalBlobSize = actualPageBlobSize;
				}else{// expand the blob
					finalBlobSize = tgtBlobSize;
					if (tgtBlobSize >= Constants.PAGEBLOB_SIZE_LIMIT){
						finalBlobSize = Constants.PAGEBLOB_SIZE_LIMIT;
					}
				}
				srcBlobSize = blob.getProperties().getLength();
				blob.resize(finalBlobSize, null, null, null);
				result = true;
				logger.trace("The size of the blob: {} is changed from {} to {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the actual size of the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BlobfsException(errMessage);
		}
		return result;
	}
	
	/* get the page blob actual size */
	public final static long getPageBlobActualLength (BlobReqParams reqParams) throws BlobfsException {
		long pageBlobActualSize = -1;
		try {
			reqParams.setBfsBlobType(BfsBlobType.PAGEBLOB);
			CloudPageBlob blob = (CloudPageBlob)getBlobReference(reqParams);
			if (blob.exists()){
				ArrayList<PageRange> pageRanges = blob.downloadPageRanges();
			    if (pageRanges.size() == 0) {
			      return pageBlobActualSize = 0;
			    }
			    pageBlobActualSize =  pageRanges.get(0).getEndOffset() - pageRanges.get(0).getStartOffset() + 1;
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the actual size of the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BlobfsException(errMessage);
		}
		return pageBlobActualSize;
	}
}
