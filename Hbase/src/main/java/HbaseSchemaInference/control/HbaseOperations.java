/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
 /*
https://github.com/larsgeorge/hbase-book/tree/master/ch05/src/main/java/admin code reference.

 */
package HbaseSchemaInference.control;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
         1 if namespace already exists
         0 on sucess 
        -1 on erro
        
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
         1 if table already exists
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 error on creation
        
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

    /*
        return 
         1 all families already exists
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 table dont exists
        -4 error on modify
        
     */
    public short alterFamilies(String namespace, String table, String[] familyName) {
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

        TableName tableName = TableName.valueOf(namespace, table);
        HTableDescriptor desc = null;
        try {
            desc = admin.getTableDescriptor(tableName);
        } catch (IOException ex) {
            return -3;
        }
        boolean allFamilyExists = true;
        for (String family : familyName) {
            HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(family));
            if (!desc.hasFamily(Bytes.toBytes(family))) {
                allFamilyExists = false;
                desc.addFamily(coldef);
            }
        }
        if (allFamilyExists) {
            return 1;
        }
        try {
            admin.modifyTable(tableName, desc);
        } catch (IOException ex) {
            return -4;
        }

        return 0;

    }

    /*
        return
        0 on sucess
        -1 table not founded
        -2 put error
    
     */
    public short putData(String namespace, String table, String row, byte[] family, byte[] column, byte[] value) {
        Connection connection = connect();
        TableName tableName = TableName.valueOf(namespace, table);
        byte[] rowName = Bytes.toBytes(row);
        Put p = new Put(rowName);
        Table tableClass = null;
        try {
            tableClass = connection.getTable(tableName);
        } catch (IOException ex) {
            return -1;
        }
        p.addColumn(family, column, value);
        try {
            tableClass.put(p);
        } catch (IOException ex) {
            return -2;
        }
        return 0;

    }

    /*
        return 
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 error on disable and delete
        
     */
    public short deleteTable(String namespace, String table) {
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

        TableName tableName = TableName.valueOf(namespace, table);

        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException ex) {
            return -3;
        }
        return 0;
    }


    /*
        return the table names of a namespace
     */
    public String[] getTables(String namespace) {
        Admin admin = null;
        try {

            Connection connection = connect();
            admin = connection.getAdmin();

        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return null;

        }
        NamespaceDescriptor namespaceDescriptor;
        try {
            namespaceDescriptor = admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {
            return null;
        }

        HTableDescriptor[] tables = new HTableDescriptor[0];
        try {
            tables = admin.listTableDescriptorsByNamespace(namespace);
        } catch (IOException ex) {
            return null;
        }
        String[] tableNames = new String[tables.length];
        for (int i = 0; i < tables.length; i++) {
            tableNames[i] = tables[i].getNameAsString().split(":")[1];

        }
        return tableNames;
    }

    /*
        return the table names of a namespace
     */
    public String[] getfamilies(String namespace, String table) {
        Admin admin = null;
        try {

            Connection connection = connect();
            admin = connection.getAdmin();

        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return null;

        }
        
        TableName tableName = TableName.valueOf(namespace, table);
        HTableDescriptor desc = null;
        try {
            desc = admin.getTableDescriptor(tableName);
        } catch (IOException ex) {
            return null;
        }

        HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();
        String[] familyNames = new String[columnFamilies.length];
        for (int j = 0; j < columnFamilies.length; j++) {
            familyNames[j] = columnFamilies[j].getNameAsString();
        }

        return familyNames;
    }

}
