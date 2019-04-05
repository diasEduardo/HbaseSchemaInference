/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
 /*
https://github.com/larsgeorge/hbase-book/tree/master/ch05/src/main/java/admin code reference.

 */
package com.mycompany.hbase;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class HbaseOperations {

    public User getUser() throws IOException {
        return User.getCurrent();
    }

    /*
        return a connection or null on erro.
     */
    public Connection connect() {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection();
            return connection;
        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /*
        return 
        -1 on erro
        0 on sucess and
        1 if namespace already exists
     */
    public short createNamespace(String name) {
        Admin admin = null;
        try {

            Connection connection = connect();
            admin = connection.getAdmin();

        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return -1;

        }

        try {
            admin.getNamespaceDescriptor(name);
            return 1;
        } catch (IOException ex) {
            try {
                NamespaceDescriptor namespace = NamespaceDescriptor.create(name).build();
                admin.createNamespace(namespace);
            } catch (IOException ex1) {
                Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return 0;

    }

    /*
        return 
        -1 on erro
        -2 namespace dont exists
        0 on sucess and
        1 if table already exists
     */
    public short createTable(String namespace, String name, String[] familyName) {
        Admin admin = null;
        try {

            Connection connection = connect();
            admin = connection.getAdmin();

        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return -1;

        }

        try {
            admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {
            return -2;
        }

        TableName tableName = TableName.valueOf(namespace, name);

        try {
            admin.getTableDescriptor(tableName);
            return 1;
        } catch (IOException ex) {
        }

        HTableDescriptor desc = new HTableDescriptor(tableName);

        for (String family : familyName) {
            HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(family));
            desc.addFamily(coldef);
        }
        try {
            admin.createTable(desc);
        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return -3;
        }

        return 0;

    }
}
