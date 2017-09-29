package com.wesley.blobfs;

import java.util.Calendar;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BfsCacheBase {
	
	protected  ConcurrentHashMap<String, CachedObject> cacheStore = null;
	protected  int capacity;
	protected  int expireTime;
   
	protected boolean put(String cacheKey, Object cached) throws BlobfsException{
    	if (!trimToCapcity()){
    		String errMessage = "Cache is full when puting the key: " + cacheKey + ".";
    		throw new BlobfsException(errMessage);
    	}
    	Calendar timeout = Calendar.getInstance();
        timeout.setTimeInMillis(timeout.getTimeInMillis() + expireTime);
        CachedObject cacheObject = new CachedObject(cached, timeout);
        cacheStore.put(cacheKey, cacheObject);
        return true;
    }
	
	protected boolean put(Long cacheKey, Object cached) throws BlobfsException{
		return put(Long.toString(cacheKey), cached);
	}
    
	protected Object get(String cacheKey){
        if(this.has(cacheKey)){
            CachedObject cachedObject = cacheStore.get(cacheKey);
            if(cachedObject.expire != null){
                if(Calendar.getInstance().before(cachedObject.expire)){
                    return cachedObject.cachedObject;
                }else{
                    return null;
                }
            }
            return cachedObject.cachedObject;
        }
        return null;
    }
	
	protected Object get(Long cacheKey){
		return get(Long.toString(cacheKey));
	}
    
	protected boolean trimToCapcity(){
    	boolean result = false;
    	if (this.count() <= capacity || this.isEmpty()) {
            return true;
        }
    	for (Entry<String, CachedObject> toRemove : cacheStore.entrySet()) {
	        CachedObject cachedObject = toRemove.getValue();
    		if(cachedObject.expire != null){
                if(Calendar.getInstance().after(cachedObject.expire)){
        	        this.delete(toRemove.getKey());
        	    }
            }
    	}
    	if (this.count() <= capacity){
    		result = true;    		
    	}
    	return result;
    	
    }
    
	protected boolean has(String cacheKey){
        return cacheStore.containsKey(cacheKey);
    }
	
	protected boolean has(long cacheKey){
		return has(Long.toString(cacheKey));
    }
	
	protected boolean isEmpty(){
        return cacheStore.isEmpty();
    }
    
	protected void delete(String cacheKey){
        cacheStore.remove(cacheKey);
    }
	
	protected void delete (Long cacheKey){
		delete(Long.toString(cacheKey));
	}
    
	protected int count(){
        return cacheStore.size();
    }
    
	protected void clear(){
        cacheStore.clear();
    }
}
/* the object that will be cached */
class CachedObject{
    Calendar expire;
    Object cachedObject;
    public CachedObject(Object cachedObject){
        this.cachedObject = cachedObject;
    }
    public CachedObject(){
    }
    public CachedObject(Object cachedObject, Calendar expire){
        this.cachedObject = cachedObject;
        this.expire = expire;
    }
}
