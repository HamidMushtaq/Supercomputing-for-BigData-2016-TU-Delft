package tudelft.utils;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import htsjdk.samtools.util.BufferedLineReader;
import htsjdk.samtools.*;

public class DictParser
{	
	SAMSequenceDictionary dict;
	
	private String getLine(FileInputStream stream) throws IOException 
	{
		String tmp = "";
		int nlines = 0;
		try 
		{
			int c = stream.read();
			while(((char)c != '\n') && (c != -1)) 
			{
				tmp = tmp + (char)c;
				c = stream.read();
			}
			//System.err.println("|" + tmp + "|");
			return (c == -1)? null : tmp;
		} 
		catch (Exception ex) 
		{
			System.err.println("End of dict\n");
			return null;
		}
	}
		
	public SAMSequenceDictionary parse(String dictPath) 
	{
		try 
		{
			// Note: Change the file system according to the platform you are running on.
			FileInputStream stream = new FileInputStream(new File(dictPath));
			String line = getLine(stream); // header
			dict = new SAMSequenceDictionary();
			line = getLine(stream);
			while(line != null) 
			{
				// @SQ	SN:chrM	LN:16571
				String[] lineData = line.split("\\s+");
				String seqName = lineData[1].substring(lineData[1].indexOf(':') + 1);
				int seqLength = 0;
				try 
				{
					seqLength = Integer.parseInt(lineData[2].substring(lineData[2].indexOf(':') + 1));
				} 
				catch(NumberFormatException ex) 
				{
					System.out.println("Number format exception!\n");
				}
				SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
	//                Logger.DEBUG("name: " + seq.getSequenceName() + " length: " + seq.getSequenceLength());
				dict.addSequence(seq);  
				line = getLine(stream);
			}
			stream.close();
			return dict;
		} 
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
}