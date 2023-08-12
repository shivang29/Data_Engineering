package Spark;
import java.io.Serializable;

public class TotalSales implements Serializable{
    private String orderID;
    private String orderDate;
    private String shipMode;
    private String segment;
    private String city;
    private String state;
    private String country;
    private String category;
    private String subCategory;
    private double sales;
    private int quantity;
    private double discount;
    private String profit;
    private double shippingCost;
    private String returns;

    public TotalSales(String orderID, String orderDate, String shipMode, String segment,
                      String city, String state, String country, String category, String subCategory,
                      double sales, int quantity, double discount, String profit,
                      double shippingCost, String returns) {
        this.orderID = orderID;
        this.orderDate = orderDate;
        this.shipMode = shipMode;
        this.segment = segment;
        this.city = city;
        this.state = state;
        this.country = country;
        this.category = category;
        this.subCategory = subCategory;
        this.sales = sales;
        this.quantity = quantity;
        this.discount = discount;
        this.profit = profit;
        this.shippingCost = shippingCost;
        this.returns = returns;
    }
    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getShipMode() {
        return shipMode;
    }

    public void setShipMode(String shipMode) {
        this.shipMode = shipMode;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    public double getSales() {
        return sales;
    }

    public void setSales(double sales) {
        this.sales = sales;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    public String getProfit() {
        return profit;
    }

    public void setProfit(String profit) {
        this.profit = profit;
    }

    public double getShippingCost() {
        return shippingCost;
    }

    public void setShippingCost(double shippingCost) {
        this.shippingCost = shippingCost;
    }

    public String getReturns() {
        return returns;
    }

    public void setReturns(String returns) {
        this.returns = returns;
    }

    // toString method for easy debugging and printing
    @Override
    public String toString() {
        return "TotalSales{" +
                "orderID='" + orderID + '\'' +
                ", orderDate='" + orderDate + '\'' +
                ", shipMode='" + shipMode + '\'' +
                ", segment='" + segment + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", country='" + country + '\'' +
                ", category='" + category + '\'' +
                ", subCategory='" + subCategory + '\'' +
                ", sales=" + sales +
                ", quantity=" + quantity +
                ", discount=" + discount +
                ", profit=" + profit +
                ", shippingCost=" + shippingCost +
                ", returns='" + returns + '\'' +
                '}';
    }
}
