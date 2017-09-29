/**
 */
package com.wesley.blobfs.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.blob.BlockEntry;
import com.wesley.blobfs.BlobfsException;
import com.wesley.blobfs.Constants;


/**
 * A class which provides utility methods
 * 
 */
/**
 * @author weswu
 *
 */
/**
 * @author weswu
 *
 */
public final class BfsUtility {
    
	private static Logger logger = LoggerFactory.getLogger("BlobOps.class");

    
    /**
     * Prints out the exception information .
     */
    public static void printException(Throwable t) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        System.out.println(String.format(
                "Got an exception from running samples. Exception details:\n%s\n",
                stringWriter.toString()));
    }

    /**
     * Prints out the sample start information .
     */
    public static void printSampleStartInfo(String sampleName) {
        System.out.println(String.format(
                "The Azure storage client library sample %s starting...",
                sampleName));
    }

    /**
     * Prints out the sample complete information .
     */
    public static void printSampleCompleteInfo(String sampleName) {
        System.out.println(String.format(
                "The Azure storage client library sample %s completed.",
                sampleName));
    }

    /**
     * @param sourceDirectory : sample: /home/data/
     * @param sourceFile: sample: app.log
     * @param splitLength: sample: 1024 * 1024 * 4 
     * @param tmpDirectory: sample: /tmp/
     * @return
     */
    @SuppressWarnings("resource")
	public static ArrayList<String> splitFileIntoChunks(String sourceDirectory, String sourceFile, long splitLength, String tmpDirectory) {
        ArrayList<String> fileChunks = new ArrayList<String>();
    	/* count the content length for each chunk */
    	long chunkLenghCounter = 0;
    	/* the index of chunk counter */
        int chunkCounter = 0;
        String randomString = Long.toHexString(Double.doubleToLongBits(Math.random()));
        String souceFileFullPath  = sourceDirectory + Constants.PATH_DELIMITER + sourceFile;
        String localTmpDir = (null == tmpDirectory) ? System.getProperty("java.io.tmpdir") : tmpDirectory + Constants.PATH_DELIMITER;
        String tmpFileFullPathPrefix = localTmpDir + sourceFile + "." + randomString;
    	String lineData = null;
    	try {
	    	 FileReader fileRead = new FileReader(souceFileFullPath);
             BufferedReader bufferedReader = new BufferedReader(fileRead);
             FileWriter outPutFile = null;
             BufferedWriter outPutBuffer = null;
             boolean firstChunk = true;
             while((lineData= bufferedReader.readLine()) != null){
            	 /* this is first line, so we should create the first chunk file */
            	 if(firstChunk){
            		 String tmpFileFullPath  = tmpFileFullPathPrefix + "." + chunkCounter;
            		 fileChunks.add(tmpFileFullPath);
            		 outPutFile = new FileWriter(tmpFileFullPath, true);
    	        	 outPutBuffer = new BufferedWriter(outPutFile);
    	        	 firstChunk = false;
            	 }
            	 /* add the length of current line to the counter */
            	 chunkLenghCounter += lineData.length();
            	 /*  the length reach the split length, so we should create a new chunk file */
            	 if (!firstChunk && chunkLenghCounter > splitLength){
            		 /* close previous handler */
            		 if (outPutBuffer != null){outPutBuffer.flush(); outPutBuffer.close();}
            		 if (outPutFile != null){outPutFile.close();}
            		 /* increase the count */
            		 chunkCounter ++;
            		 /* reset the length, count the length into the new chunks*/
            		 chunkLenghCounter = lineData.length(); /* count the length into the new chunks*/
            		 /* create the new file */
            		 String tmpFileFullPath  = tmpFileFullPathPrefix + "." + chunkCounter;
            		 fileChunks.add(tmpFileFullPath);
            		 outPutFile = new FileWriter(tmpFileFullPath, true);
    	        	 outPutBuffer = new BufferedWriter(outPutFile);            		 
    	        	 /* write the line */
    	        	 outPutBuffer.write(lineData);
            		 outPutBuffer.newLine();             		
            		 continue; /* go to next loop */
            	 }
            	 /* write the date into the chopped file */
            	 outPutBuffer.write(lineData);
	    		 outPutBuffer.newLine();
             }
             /* we should close the handler, otherwise the last handler may not write the data into the file */
             if (outPutBuffer != null){outPutBuffer.flush(); outPutBuffer.close();}
    		 if (outPutFile != null){outPutFile.close();}
        
    	} catch (Exception e) {
    		logger.error("Unexpected exception occurred when spliting the file: {} ,{}.",souceFileFullPath,e.getMessage());
		    System.err.println(e.getMessage());
	    }
    	return fileChunks;
    }
	public static long[] splitFileAndGetChunksPosition(String filePath, long splitLength) {
    	/* set the counter */
        File srcFile = new File(filePath);
    	long srcfileSize = (long)srcFile.length();
        int chunks = (int)((float)srcfileSize / (float)splitLength) + 1;
    	long[] offsets = new long[chunks];
        // determine line boundaries for number of chunks
        RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(srcFile, "r");
			for (int i = 1; i < chunks; i++) {
	             raf.seek(i * srcfileSize / chunks);
	             //System.out.println("begin pointer:" + raf.getFilePointer());
//	             while(raf.readLine() != null){
//                     break;
//	             }
	             while (true) {
	                 int read = raf.read();
	                 if (read == '\n' || read == -1) {
	                	 //System.out.println("line breaker:" + read);
	                     break;
	                 }
	             }
	             offsets[i] = raf.getFilePointer();
	         }
		} catch (FileNotFoundException e) {
			logger.error("FileNotFoundException occurred when opening the file: {} ,{}.",filePath,e.getMessage());
		    System.err.println(e.getMessage());
		}catch (IOException e) {
			logger.error("IOException occurred when opening the file: {} ,{}.",filePath,e.getMessage());
		    System.err.println(e.getMessage());
		}
		return offsets;
    }
	/* formate file size */
	public static String formatFileSize(long v) {
	    if (v < 1024) return v + " B";
	    int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
	    return String.format("%.1f %sB", (double)v / (1L << (z*10)), " KMGTPE".charAt(z));
	}
	
	
	/**
	 * the exception handle function of the blobfs
	 * @param ex
	 * @param errMessage
	 * @throws BlobfsException
	 */
	public static void throwBlobfsException(Exception ex, String errMessage) throws BlobfsException{
		String finalErrMsg = errMessage;
		if (ex instanceof BlobfsException){
			finalErrMsg = ex.getMessage();
		}
		throw new BlobfsException(finalErrMsg);
	}
	
	/**
	 * change a list of BlickEntry's block id into a string.
	 * @param blockList
	 * @return
	 */
	public static String blockIds(List<BlockEntry> blockList){
		StringBuilder sb = new StringBuilder();
		for (BlockEntry b : blockList)
		{
		    sb.append(b.getId());
		    sb.append(",");
		}
		return sb.toString();
	}
	
    /**
     * Execute a command and returns the (standard) output through a StringBuilder.
     *
     * @param command The command
     * @param output The output
     * @return The exit code of the command
     * @throws IOException if an I/O error is detected.
     */
    public static int commandExecutor(final String command, final StringBuilder output) throws IOException
    {
        final Process process = Runtime.getRuntime().exec(command);
        final InputStreamReader stream = new InputStreamReader(process.getInputStream());

        // Read the stream
        final char[] buffer = new char[Constants.COMMAND_EXECUTION_BUFFER_SIZE];
        int read;
        while ((read = stream.read(buffer, 0, buffer.length)) >= 0)
        {
            output.append(buffer, 0, read);
        }
        stream.close();
        // Wait until the command finishes (should not be long since we read the output stream)
        while (process.isAlive())
        {
            try
            {
                Thread.sleep(Constants.DEFAULT_THREAD_SLEEP_MILLS);
            }
            catch (final Exception ex)
            {
            	String errMessage = "Exception occurred when execute the command: " + command + ". " + ex.getMessage();
    			BfsUtility.throwBlobfsException(ex, errMessage);
            }
        }
        process.destroy();
        return process.exitValue();
    }
	
	/**
	 * get uid and gid on unix
	 * @return
	 * @throws IOException
	 */
	public static int getIdOnUnix(String cmd) throws IOException{
		int uid = -1;
		final StringBuilder output = new StringBuilder();
		int exitValue = commandExecutor(cmd, output);
		if (exitValue == 0 && null != output){
			uid = Integer.parseInt(output.toString().trim());
		}
		return uid;		
	}
	
	public static String removeLastSlash (String path){
		String tmpPath = "";
		if (path.endsWith("/")) {
			tmpPath = path.substring(0, path.length() - 1);
		} else {
			tmpPath = path;
		}
		return tmpPath;
	}
	
	public static String changeDateTimeZone(Date date , String timeZone){
		DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		return dateFormat.format(date);
	}
	
}
