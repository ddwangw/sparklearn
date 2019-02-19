import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class SplitBigTXTFile {
	 public static void main(String[] args){
		 splitDataToSaveFile(1000000,"E:\\jtkk\\jtkk.txt","E:\\jtkk\\");
	 }

	/**
	 * 按行分割文件
	 * @param rows 为多少行一个文件 
	 * @param sourceFilePath 为源文件路径 
	 * @param targetDirectoryPath 文件分割后存放的目标目录
	 */
	public static void splitDataToSaveFile(int rows, String sourceFilePath,
			String targetDirectoryPath) {
		
		File sourceFile = new File(sourceFilePath);
		File targetFile = new File(targetDirectoryPath);
		if (!sourceFile.exists() || rows <= 0 || sourceFile.isDirectory()) {
			return;
		}
		if (targetFile.exists()) {
			if (!targetFile.isDirectory()) {
				return;
			}
		} else {
			targetFile.mkdirs();
		}
		try {
 
			InputStreamReader in = new InputStreamReader(new FileInputStream(sourceFilePath),"UTF-8");
			BufferedReader br=new BufferedReader(in);
			
			BufferedWriter bw = null;
//			String str = "";
			String tempData = br.readLine();
			StringBuffer sb = new StringBuffer();
			int i = 1, s = 0;
			while (tempData != null) {
//				str += tempData + "\r\n";
				sb.append(tempData + "\r\n");
				if (i % rows == 0) {
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream( 
							targetFile.getAbsolutePath() + "/" +  sourceFile.getName() +"_" + (s+1) +".txt"), "UTF-8"),1024); 
					
					bw.write(sb.toString());
					bw.close();
					
//					str = "";
					sb.setLength(0);
					s += 1;
				}
				if(i % rows == 0) {
					System.out.println("已加载"+i);
				}
				i++;
				tempData = br.readLine();
			}
			if ((i - 1) % rows != 0) {
				
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream( 
						targetFile.getAbsolutePath() + "/" +  sourceFile.getName() +"_" + (s+1) +".txt"), "UTF-8"),1024); 
				bw.write(sb.toString());
				bw.close();
				br.close();
				
				s += 1;
			}
			in.close();
			
		} catch (Exception e) {
		}
	}
}
