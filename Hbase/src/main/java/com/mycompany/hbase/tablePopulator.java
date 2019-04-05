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
    ArrayList<String> families;

    public tablePopulator(String namespace, String table, int numFamilies) {
        this.namespace = namespace;
        this.table = table;
        families = new  ArrayList<String>();
        for (int i = 0; i < numFamilies; i++) {
            families.add("f"+i);
        }
    }

}
