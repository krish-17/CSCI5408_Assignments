package task1;

public class Sequence4 extends Thread {
    private String[] seqQuery;
    ConnectionUtil[] cu;
    TransactionImpl timpl;

    public Sequence4(TransactionImpl timpl, ConnectionUtil[] cu) {
        this.seqQuery = new String[] {SqlQueryConstants.selectQuery, SqlQueryConstants.selectQuery, ""};
        this.timpl = timpl;
        this.cu = cu;
    }

    @Override
    public void run() {

        this.timpl.executeQueryStatement(this.seqQuery,"4" ,this.cu);
    }
}
