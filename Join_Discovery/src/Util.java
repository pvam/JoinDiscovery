import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Date;

import com.sun.java_cup.internal.runtime.Scanner;


class ResultFileWriter {
	File fileRes, fileEstimated;
	PrintWriter outActual,outEstimated;
	public ResultFileWriter(String actual,String estimated) {
		new File("Results").mkdir();
		fileRes= new File("Results/"+actual);
		fileEstimated = new File("Results/"+estimated);
		try {
			
			outActual = new PrintWriter(fileRes);
			outEstimated = new PrintWriter(fileEstimated);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void close() {
		outActual.close();
		outEstimated.close();
	}
	public void writeToFile(String s) {
		System.out.print(s);
		if(Main.isActual) 
			outActual.print(s);
		else 
			outEstimated.print(s);
	}
}

