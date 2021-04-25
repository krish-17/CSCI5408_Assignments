package task1;

import java.sql.*;
public class Sequence3 extends Thread {

    private String seqQuery[];
    ConnectionUtil[] cu;
    TransactionImpl timpl;


    public Sequence3(TransactionImpl timpl, ConnectionUtil[] cu) {
        this.seqQuery = new String[] {SqlQueryConstants.selectQuery, SqlQueryConstants.selectQuery, ""};
        this.timpl = timpl;
        this.cu = cu;
    }

    @Override
    public void run() {

        this.timpl.executeQueryStatement(this.seqQuery,"3" ,this.cu);
    }


}
