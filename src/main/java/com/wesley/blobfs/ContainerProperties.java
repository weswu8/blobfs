package com.wesley.blobfs;

import java.util.Date;

public class ContainerProperties {

	private String name;
	private Date created;
	private Date lastModified;
		
	public ContainerProperties() {
	}
		
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
	
}
