/* 
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
package tudelft.utils;

import htsjdk.samtools.*;
import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;

public class Configuration implements Serializable
{
	private String refFolder;
	private String toolsFolder;
	private String tmpFolder;
	private String inputFolder;
	private String outputFolder;
	private String numInstances;
	private String numThreads;
	private double scc;
	private double sec;
	private SAMSequenceDictionary dict;
	private Long startTime;
	
	public void initialize()
	{	
		try
		{
			File file = new File("config.xml");
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			refFolder = correctFolderName(document.getElementsByTagName("refFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			numInstances = document.getElementsByTagName("numInstances").item(0).getTextContent();
			numThreads = document.getElementsByTagName("numThreads").item(0).getTextContent();

			scc						= 30.0;
			sec						= 30.0;
			startTime				= System.currentTimeMillis();
			
			print();
			
			DictParser dictParser = new DictParser();
			dict = dictParser.parse("./ucsc.hg19.dict");
			System.out.println("\n1.Hash code of dict = " + dict.hashCode() + "\n");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
		if (r.charAt(r.length() - 1) != '/')
			return r + '/';
		else
			return r;
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
	
	public String getRefFolder()
	{
		return refFolder;
	}
	
	public String getToolsFolder()
	{
		return toolsFolder;
	}
	
	public String getTmpFolder()
	{
		return tmpFolder;
	}
	
	public String getInputFolder()
	{
		return inputFolder;
	}
	
	public String getOutputFolder()
	{
		return outputFolder;
	}
	
	public String getNumInstances()
	{
		return numInstances;
	}
	
	public String getNumThreads()
	{
		return numThreads;
	}
	
	public void setNumInstances(String numInstances)
	{
		this.numInstances = numInstances;
	}
	
	public void setNumThreads(String numThreads)
	{
		this.numThreads = numThreads;
	}
	
	public String getSCC()
	{
		Double x = scc;
		
		return x.toString();
	}
	
	public String getSEC()
	{
		Double x = sec;
		
		return x.toString();
	}
	
	public Long getStartTime()
	{
		return startTime;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("refFolder:\t" + refFolder);
		System.out.println("toolsFolder:\t" + toolsFolder);
		System.out.println("tmpFolder:\t" + tmpFolder);
		System.out.println("inputFolder:\t" + inputFolder);
		System.out.println("outputFolder:\t" + outputFolder);
		System.out.println("numInstances:\t" + numInstances);
		System.out.println("numThreads:\t" + numThreads);
		System.out.println("*************************");
	}
}