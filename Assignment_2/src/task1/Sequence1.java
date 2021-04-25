package task1;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Sequence1 extends Thread {
    private String[] seqQuery;
    ConnectionUtil[] cu;
    TransactionImpl timpl;
    public Sequence1(TransactionImpl timpl, ConnectionUtil[] cu) {
        this.seqQuery = new String[] {SqlQueryConstants.selectQuery, SqlQueryConstants.selectQuery, ""};
        this.timpl = timpl;
        this.cu = cu;
    }

    @Override
    public void run() {
            this.timpl.executeQueryStatement(this.seqQuery,"1" ,this.cu);
    }
}
