package com.wesley.blobfs;

import java.util.concurrent.ConcurrentHashMap;

public final class BlobBufInsCache extends BfsCacheBase {
	private static BlobBufInsCache instance;
	private BlobBufInsCache(){
		cacheStore = new ConcurrentHashMap<String, CachedObject>(Constants.BLOB_BUFFER_INS_CACHE_INIT_CAPACITY, 0.9f, 1);
		capacity = Constants.BLOB_BUFFER_INS_MAX_CAPACITY;
		expireTime = Constants.BLOB_BUFFER_INS_EXPIRE_TIME;
	}
	
	public static BlobBufInsCache getInstance(){
        if(instance == null){
            synchronized (BlobBufInsCache.class) {
                if(instance == null){
                    instance = new BlobBufInsCache();
                }
            }
        }
        return instance;
    }
}
