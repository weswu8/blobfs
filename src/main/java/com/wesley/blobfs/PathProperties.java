package com.wesley.blobfs;

import java.util.Date;

public class PathProperties {
	private long inode;
	private String name;
	private BfsPathType bfsPathType;
	private BfsBlobType bfsBlobType;
	private Date mtime;
	private Date ctime;
	private long size = 0L;
	private int uid;
	private int gid;
	
	public PathProperties() {
	}

	public long getInode() {
		return inode;
	}

	public void setInode(long inode) {
		this.inode = inode;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public BfsPathType getBfsPathType() {
		return bfsPathType;
	}

	public void setBfsPathType(BfsPathType bfsPathType) {
		this.bfsPathType = bfsPathType;
	}
	
	public BfsBlobType getBfsBlobType() {
		return bfsBlobType;
	}

	public void setBfsBlobType(BfsBlobType bfsBlobType) {
		this.bfsBlobType = bfsBlobType;
	}

	public Date getMtime() {
		return mtime;
	}

	public void setMtime(Date mtime) {
		this.mtime = mtime;
	}

	public Date getCtime() {
		return ctime;
	}

	public void setCtime(Date ctime) {
		this.ctime = ctime;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public int getGid() {
		return gid;
	}

	public void setGid(int gid) {
		this.gid = gid;
	}	
	
}
