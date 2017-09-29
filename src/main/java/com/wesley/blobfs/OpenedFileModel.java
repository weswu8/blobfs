package com.wesley.blobfs;

import java.io.Closeable;
import java.io.IOException;


public class OpenedFileModel extends CachedObject implements Closeable {
	private String leaseID;
	private String container;
	private String blob;
	private BlobBufferedIns bIn;
	private BlobBufferedOus bOut;
	
	public OpenedFileModel(BlobBufferedIns in, BlobBufferedOus out) {
		bIn = in;
		bOut = out;
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


	public String getLeaseID() {
		return leaseID;
	}


	public void setLeaseID(String leaseID) {
		this.leaseID = leaseID;
	}


	public BlobBufferedIns getbIn() {
		return bIn;
	}

	public BlobBufferedOus getbOut() {
		return bOut;
	}


	@Override
	public void close() throws IOException {
	    if (bIn != null) {
	    	bIn.close();
	    }

	    if (bOut != null) {
	    	bOut.close();
	    }
		
	}
}
