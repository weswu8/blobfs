package com.wesley.blobfs;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;

public class BlobReqParams {
	
	BfsBlobType bfsBlobType;
	String container;
	String blob;
	CloudBlob blobInstance;
	String destContainer;
	String destBlob;
	String LocalTmpDir;
	String localDir;
	String localFile;
	String leaseID;
	String content;
	Long localFileSize;
	BlobOptMode blobOptMode;
	VirtualDirOptMode virtualDirOptMode;
	boolean doForoce;
	Long blobSize;
	long offset;
	long length;
	AccessCondition accessConditon;
	BlobRequestOptions blobRequestOptions;
	OperationContext operationContext;
	
	public String getBlobFullPath(){
		return container + Constants.PATH_DELIMITER + blob;
	}
	
	public String getDestBlobFullPath(){
		return destContainer + Constants.PATH_DELIMITER + destBlob;
	}
	
	public String getLocalFileFullPath(){
		//return localDir + Constants.PATH_DELIMITER + localFile;
		return localDir + localFile;
	}	

	public BfsBlobType getBfsBlobType() {
		return bfsBlobType;
	}

	public void setBfsBlobType(BfsBlobType bfsBlobType) {
		this.bfsBlobType = bfsBlobType;
	}

	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}

	public String getBlob() {
		return blob;
	}

	public void setBlob(String blob) {
		this.blob = blob;
	}
		
	public CloudBlob getBlobInstance() {
		return blobInstance;
	}

	public void setBlobInstance(CloudBlob blobInstance) {
		this.blobInstance = blobInstance;
	}

	public String getDestContainer() {
		return destContainer;
	}

	public void setDestContainer(String destContainer) {
		this.destContainer = destContainer;
	}

	public String getDestBlob() {
		return destBlob;
	}

	public void setDestBlob(String destBlob) {
		this.destBlob = destBlob;
	} 

	public String getLocalTmpDir() {
		return LocalTmpDir;
	}

	public void setLocalTmpDir(String localTmpDir) {
		LocalTmpDir = localTmpDir;
	}

	public String getLocalDir() {
		return localDir;
	}

	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}

	public String getLocalFile() {
		return localFile;
	}

	public void setLocalFile(String localFile) {
		this.localFile = localFile;
	}
	
	public String getLeaseID() {
		return leaseID;
	}

	public void setLeaseID(String leaseID) {
		this.leaseID = leaseID;
	}	
	
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Long getLocalFileSize() {
		return localFileSize;
	}

	public void setLocalFileSize(long localFileSize) {
		this.localFileSize = localFileSize;
	}

	public VirtualDirOptMode getVirtualDirOptMode() {
		return virtualDirOptMode;
	}

	public BlobOptMode getBlobOptMode() {
		return blobOptMode;
	}

	public void setBlobOptMode(BlobOptMode blobOptMode) {
		this.blobOptMode = blobOptMode;
	}

	public void setVirtualDirOptMode(VirtualDirOptMode virtualDirOptMode) {
		this.virtualDirOptMode = virtualDirOptMode;
	}

	public boolean isDoForoce() {
		return doForoce;
	}

	public void setDoForoce(boolean doForoce) {
		this.doForoce = doForoce;
	}
	

	public Long getBlobSize() {
		return blobSize;
	}

	public void setBlobSize(long blobSize) {
		this.blobSize = blobSize;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public AccessCondition getAccessConditon() {
		return accessConditon;
	}

	public void setAccessConditon(AccessCondition accessConditon) {
		this.accessConditon = accessConditon;
	}

	public BlobRequestOptions getBlobRequestOptions() {
		return blobRequestOptions;
	}

	public void setBlobRequestOptions(BlobRequestOptions blobRequestOptions) {
		this.blobRequestOptions = blobRequestOptions;
	}

	public OperationContext getOperationContext() {
		return operationContext;
	}

	public void setOperationContext(OperationContext operationContext) {
		this.operationContext = operationContext;
	}

	public void setBlobSize(Long blobSize) {
		this.blobSize = blobSize;
	}	
	
}
