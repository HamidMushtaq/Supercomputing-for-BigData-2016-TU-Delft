/****************************************/
//	Class Name:	HDFSManager	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package tudelft.utils;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.net.*;
import java.lang.*;
import java.nio.charset.Charset;

public class HDFSManager
{
	public static void create(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			fs = FileSystem.get(config);
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static void append(String hadoopInstall, String fname, String s)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{			
			FSDataOutputStream fout = FileSystem.get(config).append(new Path(fname));
			PrintWriter writer = new PrintWriter(fout);
			writer.append(s);
			writer.close();
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static String readWholeFile(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{			
			FileSystem fs = FileSystem.get(config);
			StringBuilder builder=new StringBuilder();
			byte[] buffer=new byte[8192000];
			int bytesRead;
  
			FSDataInputStream in = fs.open(new Path(fname));
			while ((bytesRead = in.read(buffer)) > 0) 
				builder.append(new String(buffer, 0, bytesRead, "UTF-8"));
			in.close();
			
			return builder.toString();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return "";
        }
	}
	
	public static void writeWholeFile(String hadoopInstall, String fname, String s)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			Charset UTF8 = Charset.forName("utf-8");
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(fout, UTF8));
			writer.write(s);
			writer.flush();
			writer.close();
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public String getLS(String dir, boolean showHidden)
	{
		try
		{
			File folder = new File(dir);
			File[] listOfFiles = folder.listFiles();
			String lenStr = Integer.toString(listOfFiles.length);
			String str = "";
			
			InetAddress IP = InetAddress.getLocalHost();
			String hostName = IP.toString();
			
			for (int i = 0; i < listOfFiles.length; i++) 
			{
				if (listOfFiles[i].isFile())
				{
					if (!(listOfFiles[i].isHidden() && !showHidden))
						str = str + String.format("%s\t%s\n", getSizeString(listOfFiles[i].length()),
							listOfFiles[i].getName());
				}
				else if (listOfFiles[i].isDirectory()) 
					str = str + "DIR:\t" + listOfFiles[i].getName() + "\n";
			}
			return "\n************\n\tNumber of files in " + dir + " of " + hostName + 
				" = " + lenStr + "\n" + str + "\n************\n";
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return "Exception in HDFSManager.getLS";
		}
	}
	
	public static int download(String hadoopInstall, String fileName, String hdfsFolder, String localFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		//config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		//config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			fs.copyToLocalFile(new Path(hdfsFolder + fileName), 
				new Path(localFolder + fileName));
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public static int downloadIfRequired(String hadoopInstall, String fileName, String hdfsFolder, String localFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			File f = new File(localFolder + fileName);
			if (!f.exists())
			{
				fs.copyToLocalFile(new Path(hdfsFolder + fileName), new Path(localFolder + fileName));
			}
			else
			{
				long localFileSize = f.length();
				long hdfsFileSize = fs.getFileStatus(new Path(hdfsFolder + fileName)).getLen();
				
				if (localFileSize != hdfsFileSize)
					fs.copyToLocalFile(new Path(hdfsFolder + fileName), new Path(localFolder + fileName));
			}
			
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public static void upload(String hadoopInstall, String fileName, String localFolder, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			fs.copyFromLocalFile(false, true, new Path(localFolder + fileName), 
				new Path(hdfsFolder + fileName));
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public static String[] getFileList(String hadoopInstall, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		
		try
		{
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(hdfsFolder));
			String[] fileNames = new String[status.length];

			for (int i=0; i < status.length; i++)
				fileNames[i] = status[i].getPath().getName();

			return fileNames;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	private String getSizeString(long len)
	{
		Float r;
		String unit;
		
		if (len > 1e9)
		{
			r = len / 1e9f;
			unit = "GB";
		}
		else if (len > 1e6)
		{
			r = len / 1e6f;
			unit = "MB";
		}
		else
		{
			r = len / 1e3f;
			unit = "KB";
		}
		
		return String.format("%.1f%s", r, unit);
	}
}
