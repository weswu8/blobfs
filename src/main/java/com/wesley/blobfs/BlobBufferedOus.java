package com.wesley.blobfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.wesley.blobfs.utils.BfsUtility;


public class BlobBufferedOus extends OutputStream {
	private  Logger logger = LoggerFactory.getLogger("BlobBufferedOus.class");
	private  CloudBlob blob;
	/* the central buffer */
	private byte[] centralBuffer;
	/* Lease ID for the blob */
	String leaseID;
	int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
	AccessCondition accCondtion = new AccessCondition();
	/* the pointer of the central buffer */
	int centralBufOffset= 0;
	int centralBufferSize = Constants.BLOB_BUFFERED_OUTS_BUFFER_SIZE;
	long totalDataWBuffered = 0;
	long totalDataUploaded = 0;
	long localFileSize = 0;
	/* the upload chunk size, the should be smaller than the buffer size */
	int chunkSizeOfBB = Constants.BLOB_BUFFERED_OUTS_BLOCKBLOB_CHUNK_SIZE;
	int chunkNumber = 0;
	int chunkSizeOfAB = Constants.BLOB_BUFFERED_OUTS_APPENDBLOB_CHUNK_SIZE;
	/* the available bytes in central buffer */
	long blobOffset = 0;
	/* the path of the blob */
	String fullBlobPath;
	/* the type of the blob */
	BfsBlobType blobType;
	/* the flag represent the state of local stream */
	boolean isLocalStreamEOF = false;
	boolean isBlockBlobClosed = false;
	boolean isAppendBlobExists = false;
	Integer numOfCommitedBlocks = null;

	/* list of all block ids we will be uploading - need it for the commit at the end */
    List<BlockEntry> blockList;
	
	@SuppressWarnings("static-access")
	public BlobBufferedOus(BlobReqParams reqParams) throws BlobfsException, StorageException {
		this.blob = BlobService.getBlobReference(reqParams);
		this.leaseID = blob.acquireLease(minLockedSec, null);
		this.accCondtion.setLeaseID(leaseID);
		this.fullBlobPath = reqParams.getBlobFullPath();
		this.centralBuffer = new byte[centralBufferSize];
		this.blobType = reqParams.getBfsBlobType();
		this.blockList = new ArrayList<BlockEntry>();
		this.localFileSize = (null != reqParams.getLocalFileSize()) ? reqParams.getLocalFileSize() : 0;
		/* set leaseID in the meta data */
		BlobReqParams smParams = new BlobReqParams();
		smParams.setBlobInstance(blob);
		smParams.setLeaseID(leaseID);
		BlobService.setBlobMetadata(smParams, Constants.BLOB_META_DATA_LEASE_ID_KEY, this.leaseID);
	}
	
	public CloudBlob getBlob() {
		return blob;
	}
	
	public String getLeaseID() {
		return leaseID;
	}
	
	@Override
	public void write(int b) throws IOException {
		/* simply call the write function */
		byte[] oneByte = new byte[1];
		oneByte[0] = (byte) b;
		write(oneByte, 0, 1);		
	}	
	@Override
	public synchronized void write(final byte[] data, final int offset, final int length) throws IOException {
		/* throw the parameters error */
		if (offset < 0 || length < 0 || length > data.length - offset) {
			throw new BlobfsException(new IndexOutOfBoundsException());
		}
		/* test the upload contions */
		verifyUploadConditions();
		
		/* write data to the buffer */
		writeToBuffer(data, offset, length);
		/* check the buffered data and the chunk size threshold */
		if (isBufferedDataReadyToUpload()){
			int numOfdataUploaded = 0;
			if ((numOfdataUploaded = uploadBlobChunk(centralBuffer, 0, centralBufOffset)) > 0){
				totalDataUploaded += numOfdataUploaded;
				/* clean the buffer */
				byte[] tempBuffer = new byte[centralBufferSize];
				centralBuffer = tempBuffer;
				/* reset the chunk count */
				centralBufOffset = 0;			
			}
		}			
	}
	/* write line function */
	public void writeLine(String line) throws IOException {
		/* simply call the write function */
		byte[] lineBytes = (line + System.getProperty("line.separator")).getBytes(Constants.DEFAULT_CHARSET);
		write(lineBytes, 0, lineBytes.length);		
	}
	/* push the data from buffer to blob */
	public synchronized final void flush() throws IOException {
		try {
			if (centralBufOffset > 0) {
				int numOfdataUploaded = 0;
				if ((numOfdataUploaded = uploadBlobChunk(centralBuffer, 0, centralBufOffset)) > 0) {
					totalDataUploaded += numOfdataUploaded;
					/* reset the chunk count */
					centralBufOffset = 0;	
				}
			}			
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when flush to buffered data to the blob: " + fullBlobPath + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	}
	@SuppressWarnings("static-access")
	@Override
	public synchronized final void close() throws IOException {
		try {
			if (isBlockBlobClosed) {
				return;
			}
			/* flush the data */
			flush();
			/* clean the buffer */
			centralBuffer = null;
			/* for block blob, commit the uploaded block and close the upload stream */
			if (totalDataUploaded > 0) {
				if (BfsBlobType.BLOCKBLOB.equals(this.blobType)) {
					((CloudBlockBlob) blob).commitBlockList(blockList, accCondtion, null, null);
				}
			}
			/* clear the meta data */
			/* set leaseID in the meta data */
			BlobReqParams rmParams = new BlobReqParams();
			rmParams.setBlobInstance(blob);
			rmParams.setLeaseID(leaseID);
			BlobService.removeBlobMetadata(rmParams, Constants.BLOB_META_DATA_LEASE_ID_KEY);
			BlobService.removeBlobMetadata(rmParams, Constants.BLOB_META_DATA_COMMITED_BLOBKS_KEY);
			
			/* reset the md5 */
			blob.getProperties().setContentMD5("");
			blob.uploadProperties(accCondtion, null, null);
			/* release the lease */			
			//if (blob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
			blob.releaseLease(accCondtion);
			//}
			logger.trace("Closed the blob output stream.");
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when closing the blob output stream " + fullBlobPath + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}finally{
			isBlockBlobClosed = true;
		}
		
	}
	/* write data to buffer */
	public synchronized final int writeToBuffer(byte[] rawData, int offset, int length){
		int numOfDataWrited = 0;
		/* the capacity of central buffer is ok */
		if ((centralBuffer.length - centralBufOffset) > length ){
			System.arraycopy(rawData, offset, centralBuffer, centralBufOffset, length);			
		} else {
			byte[] tempBuffer = new byte[centralBufOffset + length];
			System.arraycopy(centralBuffer, 0, tempBuffer, 0, centralBufOffset);
			System.arraycopy(rawData, offset, tempBuffer, centralBufOffset, length);
			centralBuffer = tempBuffer;
		}
		numOfDataWrited = length;
		centralBufOffset += numOfDataWrited;
		totalDataWBuffered += numOfDataWrited;		
		return numOfDataWrited;		
	}
	/* upload a chunk of data from to blob */
	@SuppressWarnings("static-access")
	public synchronized final int uploadBlobChunk (byte[] rawData,  int offset, int length) throws BlobfsException {
		int dataUploadedThisChunk = 0;
		try {
			/* renew the lease firstly, otherwise the lease may be expired, this will cause error */
			blob.renewLease(accCondtion);
			
			ByteArrayInputStream bInput = new ByteArrayInputStream(rawData, offset, length);
			/* update the chunk counter */
			chunkNumber ++;
			if (BfsBlobType.BLOCKBLOB.equals(this.blobType)){
				/* save chunk id in array (must be base64) */
                String chunkId = Base64.getEncoder().encodeToString(String.format("BlockId%07d", chunkNumber).getBytes(StandardCharsets.UTF_8));
                BlockEntry chunk = new BlockEntry(chunkId);
                blockList.add(chunk);               
				((CloudBlockBlob) blob).uploadBlock(chunkId, bInput, (long)length, accCondtion, null, null);
			} else if (BfsBlobType.APPENDBLOB.equals(this.blobType)){				
				((CloudAppendBlob) blob).appendBlock(bInput, (long)length, accCondtion, null, null);
			}
			bInput.close();
			dataUploadedThisChunk = length;
			/* save the block ID list , secure the data */ 
			if (BfsBlobType.BLOCKBLOB.equals(this.blobType)){
				/* set leaseID in the meta data */
				BlobReqParams cbParams = new BlobReqParams();
				cbParams.setBlobInstance(blob);
				cbParams.setLeaseID(leaseID);
				BlobService.setBlobMetadata(cbParams,Constants.BLOB_META_DATA_COMMITED_BLOBKS_KEY, BfsUtility.blockIds(blockList));
			}
			
		} catch (StorageException | IOException ex) {
			String errMessage = "Unexpected exception occurred when uploading to the blob : " 
						+ this.fullBlobPath + ", No. of chunk: " + chunkNumber + "." + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		logger.trace("Uploading to {} , blockId: {}, size {}", fullBlobPath, chunkNumber, length);
		return dataUploadedThisChunk;
	}
	
	public synchronized boolean isBufferedDataReadyToUpload() {
		boolean result = false;
		if (BfsBlobType.BLOCKBLOB.equals(this.blobType)){
			if (centralBufOffset > chunkSizeOfBB){ return result = true;}
		} else if (BfsBlobType.APPENDBLOB.equals(this.blobType)){
			if (centralBufOffset > chunkSizeOfAB){ return result = true;}
		}
		return result;
	}
	
	public synchronized void verifyUploadConditions() throws BlobfsException {
		long blobSizeLimit = 0;
		try {
			if (BfsBlobType.BLOCKBLOB.equals(this.blobType)){
				blobSizeLimit = Constants.BLOCKBLOB_SIZE_LIMIT;
				if (null == numOfCommitedBlocks){numOfCommitedBlocks = 0;}
			} else if (BfsBlobType.APPENDBLOB.equals(this.blobType)){
				blobSizeLimit = Constants.APPENDBLOB_SIZE_LIMIT;
				/* create the blob if it does not exist */
				if (!isAppendBlobExists && !blob.exists()){
					((CloudAppendBlob) blob).createOrReplace(null, null, null);
					isAppendBlobExists = true;
				}
				if (null == numOfCommitedBlocks){					
					numOfCommitedBlocks  = (null != ((CloudAppendBlob) blob).getProperties().getAppendBlobCommittedBlockCount()) 
							? ((CloudAppendBlob) blob).getProperties().getAppendBlobCommittedBlockCount() : 0;
				}
			} else if (BfsBlobType.PAGEBLOB.equals(this.blobType)){
				blobSizeLimit = Constants.PAGEBLOB_SIZE_LIMIT;
			}
			/* verify the size of local file is under the limit */
			if (localFileSize > blobSizeLimit){
				String errMessage = "The size of the source file exceeds the size limit: " + blobSizeLimit + "."; 
				throw new BlobfsException(errMessage);
			}
			
			if (numOfCommitedBlocks + chunkNumber > Constants.BLOB_BLOCK_NUMBER_LIMIT - 1){ //50000
				String errMessage = "The block count of target blob: " + fullBlobPath + " exceeds the " + Constants.BLOB_BLOCK_NUMBER_LIMIT + " count limit.";
				throw new BlobfsException(errMessage);
			}
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when verifing the upload conditons for to the blob : " 
					+ this.fullBlobPath + "." + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);

		}
		return;
	}
}
