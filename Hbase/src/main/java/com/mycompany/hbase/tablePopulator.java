/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbase;

import java.util.ArrayList;

/**
 *
 * @author eduardo
 */
public class tablePopulator {

    String namespace, table;
    ArrayList<String> families,rows;

    public tablePopulator(String namespace, String table, int numFamilies, int numRows) {
        this.namespace = namespace;
        this.table = table;
        families = new  ArrayList<String>();
        rows = new  ArrayList<String>();
        for (int i = 0; i < numFamilies; i++) {
            families.add("f"+i);
        }
        for (int i = 0; i < numRows; i++) {
            rows.add("r"+i);
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTable() {
        return table;
    }

    public ArrayList<String> getFamilies() {
        return families;
    }

    public ArrayList<String> getRows() {
        return rows;
    }
    
    //data generators
    /* datatypes
        byte
        boolean
        string
        short
        char
        float
        integer
        double
        long
        blob
    
    */
    
    
    
    
    //end data generators

}
