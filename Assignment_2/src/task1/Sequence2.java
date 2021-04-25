package task1;

import java.sql.*;
public class Sequence2 extends Thread {
    private String seqQuery[];
    ConnectionUtil[] cu;
    TransactionImpl timpl;
    public Sequence2(TransactionImpl timpl, ConnectionUtil[] cu) {
        this.seqQuery = new String[] {SqlQueryConstants.selectQuery, SqlQueryConstants.selectQuery, ""};
        this.timpl = timpl;
        this.cu = cu;
    }

    @Override
    public void run() {
        this.timpl.executeQueryStatement(this.seqQuery,"2" ,this.cu);
    }
}
