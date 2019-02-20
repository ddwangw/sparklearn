package com.ibeifeng.sparkproject.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class IOUtils {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		outPutFile("[{\"source\": \"4\",\r\n" + 
				"\"target\": \"83\"\r\n" + 
				"}]","link");
	}
	public static void outPutFile(String s,String name) {
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream( "E:\\\\jtkk\\\\"+name+".json"), "UTF-8"),1024);
			bw.write(s);
			bw.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
