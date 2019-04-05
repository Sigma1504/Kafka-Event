package com.accor.tech.utilstest;

public class DataExample {

    public String myData;
    public String blabla;
    public String custo;
    public String pay;
    public String action;

    public DataExample(String myData, String blabla) {
        this.myData = myData;
        this.blabla = blabla;
    }

    public DataExample(String myData, String custo, String pay, String action) {
        this.myData = myData;
        this.custo = custo;
        this.pay = pay;
        this.action = action;
    }
}
