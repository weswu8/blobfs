package com.wesley.blobfs;

public class BfsBlobModel {
	String blobURI;
	String blobName;
	BfsBlobType bfsBlobType;
	BlobProperties blobProperties;
	
	public BfsBlobModel() {
	}
	
	public String getBlobURI() {
		return blobURI;
	}
	public void setBlobURI(String blobURI) {
		this.blobURI = blobURI;
	}
	public BfsBlobType getBfsBlobType() {
		return bfsBlobType;
	}

	public void setBfsBlobType(BfsBlobType bfsBlobType) {
		this.bfsBlobType = bfsBlobType;
	}

	public String getBlobName() {
		return blobName;
	}
	public void setBlobName(String blobName) {
		this.blobName = blobName;
	}
	public BlobProperties getBlobProperties() {
		return blobProperties;
	}
	public void setBlobProperties(BlobProperties blobProperties) {
		this.blobProperties = blobProperties;
	}

	@Override
	public String toString() {
		return "BfsBlobModel [blobURI=" + blobURI + ", blobName=" + blobName + ", bfsBlobType=" + bfsBlobType
				+ ", blobProperties=" + blobProperties.toString() + "]";
	}

}
