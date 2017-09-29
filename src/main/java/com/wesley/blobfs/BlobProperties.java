package com.wesley.blobfs;

import java.util.Date;

import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.LeaseDuration;
import com.microsoft.azure.storage.blob.LeaseState;
import com.microsoft.azure.storage.blob.LeaseStatus;

public class BlobProperties {
	String name;
	BfsBlobType bfsBlobType;
	String contentMD5;
	String etag;
	Long length;
	Long actualLength; // for page blob
	Date created;
	Date lastModified;
	LeaseDuration leaseDuration;
	LeaseState leaseState;
	LeaseStatus leaseStatus;
	
	public BlobProperties() {
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}



	public BfsBlobType getBfsBlobType() {
		
		return bfsBlobType;
	}

	public void setBfsBlobType(BfsBlobType bfsBlobType) {
		this.bfsBlobType = bfsBlobType;
	}
	
	public void setBfsBlobType(BlobType blobType) {
		BfsBlobType bfsBlobType;
		switch (blobType.toString()){
			case "APPEND_BLOB":
				bfsBlobType = BfsBlobType.APPENDBLOB;
				break;
			case "PAGE_BLOB":
				bfsBlobType = BfsBlobType.PAGEBLOB;
				break;
			default:
				bfsBlobType = BfsBlobType.BLOCKBLOB;
				break;	
		}
		this.bfsBlobType = bfsBlobType;
	}

	public String getContentMD5() {
		return contentMD5;
	}
	public void setContentMD5(String contentMD5) {
		this.contentMD5 = contentMD5;
	}
	public String getEtag() {
		return etag;
	}
	public void setEtag(String etag) {
		this.etag = etag;
	}
	public Long getLength() {
		return length;
	}
	public void setLength(Long length) {
		this.length = length;
	}
	public Long getActualLength() {
		return actualLength;
	}
	public void setActualLength(Long actualLength) {
		this.actualLength = actualLength;
	}
	
	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getLastModified() {
		return lastModified;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}
	public LeaseDuration getLeaseDuration() {
		return leaseDuration;
	}
	public void setLeaseDuration(LeaseDuration leaseDuration) {
		this.leaseDuration = leaseDuration;
	}
	public LeaseState getLeaseState() {
		return leaseState;
	}
	public void setLeaseState(LeaseState leaseState) {
		this.leaseState = leaseState;
	}
	public LeaseStatus getLeaseStatus() {
		return leaseStatus;
	}
	public void setLeaseStatus(LeaseStatus leaseStatus) {
		this.leaseStatus = leaseStatus;
	}

	@Override
	public String toString() {
		return "BlobProperties [contentMD5=" + contentMD5 + ", etag=" + etag + ", length=" + length + ", actualLength="
				+ actualLength + ", lastModified=" + lastModified + ", leaseDuration=" + leaseDuration + ", leaseState="
				+ leaseState + ", leaseStatus=" + leaseStatus + "]";
	}
	
}
