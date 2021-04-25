package task2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;

public class WALImplementer {

    private long logId;
    private String logOperation;
    private String queryExec;
    private boolean queryState;
    private long transactionId;

    public void setWALProps(String logOperation, String query, boolean state, long transactionId) {
        this.logId = System.currentTimeMillis();
        this.logOperation = logOperation;
        this.queryState = state;
        this.transactionId = transactionId;
    }

    public void writeWAL() {
        try{
            File wal = new File("wal.txt");
            Writer walWriter = null;
            walWriter = new BufferedWriter(new FileWriter(wal, true));
            walWriter.write(this.logId + "-$-" + this.logOperation +"-$-" + this.queryState + "-$-" + this.transactionId + "-$-" +"\n");
            walWriter.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Failed logging");
        }
    }

    public void notifyRollBack(long transactionId) {
        try{
            File wal = new File("wal.txt");
            Writer walWriter = null;
            walWriter = new BufferedWriter(new FileWriter(wal, true));
            walWriter.write("Rollback happened from: " + this.transactionId + "-$-" +"\n");
            walWriter.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Failed logging");
        }
    }
}
