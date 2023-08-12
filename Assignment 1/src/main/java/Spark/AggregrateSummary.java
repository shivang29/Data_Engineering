package Spark;

public class AggregrateSummary {
    private String Year;
    private int Month;
    private String Category;
    private String SubCategory;
    private int TotalQuantitySold;
    private String TotalProfit;

    public AggregrateSummary(String year, int month, String category, String subCategory, int totalQuantitySold, String totalProfit) {
        this.Year = year;
        this.Month = month;
        this.Category = category;
        this.SubCategory = subCategory;
        this.TotalQuantitySold = totalQuantitySold;
        this.TotalProfit = totalProfit;
    }

    // Getters and setters

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        this.Year = year;
    }

    public int getMonth() {
        return Month;
    }

    public void setMonth(int month) {
        this.Month = month;
    }

    public String getCategory() {
        return Category;
    }

    public void setCategory(String category) {
        this.Category = category;
    }

    public String getSubCategory() {
        return SubCategory;
    }

    public void setSubCategory(String subCategory) {
        this.SubCategory = subCategory;
    }

    public int getTotalQuantitySold() {
        return TotalQuantitySold;
    }

    public void setTotalQuantitySold(int totalQuantitySold) {
        this.TotalQuantitySold = totalQuantitySold;
    }

    public String getTotalProfit() {
        return TotalProfit;
    }

    public void setTotalProfit(String totalProfit) {
        this.TotalProfit = totalProfit;
    }
}
