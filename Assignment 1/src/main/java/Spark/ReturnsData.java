package Spark;
import java.io.Serializable;
public class ReturnsData implements Serializable{

    private String orderID;
    private String returned;

    public ReturnsData(String orderID, String returned) {
        this.orderID = orderID;
        this.returned = returned;
    }
    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public String getReturned() {
        return returned;
    }

    public void setReturned(String returned) {
        this.returned = returned;
    }

    @Override
    public String toString() {
        return "ReturnsData{" +
                "orderID='" + orderID + '\'' +
                ", returned='" + returned + '\'' +
                '}';
    }
}
