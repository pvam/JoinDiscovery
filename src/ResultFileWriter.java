import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;


public class ResultFileWriter {
    File fileRes, fileEstimated;
    PrintWriter outActual, outEstimated;

    public ResultFileWriter(String dir) {
        new File(dir).mkdirs();
        fileRes = new File(dir+"actual.txt");
        fileEstimated = new File(dir + "estimated.txt");
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
        if (Main.isActual)
            outActual.print(s);
        else
            outEstimated.print(s);
    }
}

