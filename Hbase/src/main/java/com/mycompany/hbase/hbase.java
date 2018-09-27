/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class hbase {

    public static void main(String[] args) throws IOException, Exception {
        String namespace = "default";
        String table = "pessoas";
        String family = "dados";
        Configuration conf = HBaseConfiguration.create();
        //get_tables_and_families(namespace, conf);
        // scan_all(table, conf);
        get_columns_and_values( table,  family,  conf);
        get_columns( table,  family,  conf);
        //scan.setScanMetricsEnabled(true);
        // ScanMetrics scanMetrics = scanner.getScanMetrics();
        //List<Cell> clist = result2.listCells();
        //Cell cell = clist.get(0);
        //NavigableMap<byte[], byte[]> familyMap = result2.getFamilyMap(Bytes.toBytes("dados"));
        //List<Cell> columnCells = result2.getColumnCells(Bytes.toBytes("dados"), Bytes.toBytes("estado"));
    }

    public static void scan_all(String table, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nFound row : " + Bytes.toString(row));
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result2.getMap();

            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                byte[] family = entry.getKey();
                System.out.println("family key : " + Bytes.toString(family));

                for (Entry<byte[], NavigableMap<Long, byte[]>> entry1 : entry.getValue().entrySet()) {
                    byte[] column = entry1.getKey();
                    System.out.println("column key : " + Bytes.toString(column));

                    for (Entry<Long, byte[]> entry2 : entry1.getValue().entrySet()) {
                        byte[] cellValue = entry2.getValue();
                        System.out.println("Cell Value: " + Bytes.toString(cellValue));
                    }
                }
            }
        }
        scanner.close();
    }

    private static void get_tables_and_families(String namespace, Configuration conf) throws ZooKeeperConnectionException, IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        HTableDescriptor[] tabelas = admin.listTableDescriptorsByNamespace(namespace);
        for (int i = 0; i < tabelas.length; i++) {
            String nome_tabela = tabelas[i].getNameAsString();
            System.out.println("Tabela " + nome_tabela);
            HColumnDescriptor[] columnFamilies = tabelas[i].getColumnFamilies();
            for (int j = 0; j < columnFamilies.length; j++) {
                String nome_familia = columnFamilies[j].getNameAsString();
                System.out.println("familia de " + nome_tabela + ": " + nome_familia);
            }
        }
    }

    private static void get_columns_and_values(String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset()+family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
                byte[] value =Arrays.copyOfRange(rowArray, family_cell.getValueOffset(), family_cell.getValueOffset()+family_cell.getValueLength());
                System.out.println("Valor : " + Bytes.toString(value));
            }
        }
    }
    
    private static void get_columns(String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset()+family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
            }
        }
    }

}
