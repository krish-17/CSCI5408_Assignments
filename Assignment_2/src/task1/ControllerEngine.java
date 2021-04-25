package task1;

public class ControllerEngine {

    public static void main(String[] args) {
        try {
            TransactionImpl timpl = new TransactionImpl();
            ConnectionUtil[] cu = new ConnectionUtil[3];
            for(int i = 0; i < cu.length; i++) {
                cu[i] = new ConnectionUtil();
            }
            Sequence1 t = new Sequence1(timpl, cu);
            Sequence2 t2 = new Sequence2(timpl, cu);
            Sequence3 t3 = new Sequence3(timpl, cu);
            Sequence4 t4 = new Sequence4(timpl, cu);
            Sequence5 t5 = new Sequence5(timpl, cu);
            t.start();
            t.join();
            t2.start();
            t2.join();
            t3.start();
            t3.join();
            t4.start();
            t4.join();
            t5.start();
            t5.join();
            for(int i = 0; i < cu.length; i++) {
                cu[i].closeConnection();
            }
        } catch(Exception e) {
            e.printStackTrace();;
        }
    }
}
