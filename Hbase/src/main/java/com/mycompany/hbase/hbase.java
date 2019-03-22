/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLConnection;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tika.Tika;

/**
 *
 * @author eduardo
 */
public class hbase {

    public static void main(String[] args) throws IOException, Exception {
        String namespace = "default";
        String table = "pessoas";
        String family = "teste";//"dados";
        Configuration conf = HBaseConfiguration.create();
        typeTests(namespace, table, family, conf);
        //dataTests(namespace, table, family, conf);
        //binaryTests(namespace, table, family, conf);
        //get_tables_and_families(namespace, conf);
        // scan_all(table, conf);
        //get_columns_and_values(table, family, conf);
        //get_columns(table, family, conf);
        //scan.setScanMetricsEnabled(true);
        // ScanMetrics scanMetrics = scanner.getScanMetrics();
        //List<Cell> clist = result2.listCells();
        //Cell cell = clist.get(0);
        //NavigableMap<byte[], byte[]> familyMap = result2.getFamilyMap(Bytes.toBytes("dados"));
        //List<Cell> columnCells = result2.getColumnCells(Bytes.toBytes("dados"), Bytes.toBytes("estado"));
        char teste = '9';
        Bytes.toBytes(teste);
        int tint = 57;
        Bytes.toBytes(tint);
        System.out.println(teste + "bin " + Bytes.toStringBinary(Bytes.toBytes(teste)));
        System.out.println(tint + "bin " + Bytes.toStringBinary(Bytes.toBytes(tint)));
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
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(), family_cell.getValueOffset() + family_cell.getValueLength());
                int typeByte = value[0];
                if (typeByte == 51) {
                    System.out.println("string");
                } else if (typeByte == 43) // do operation for Integer
                {
                    System.out.println("int");
                } else if (typeByte == 45) {
                    System.out.println("double");
                }
                // do operation for DoubleF
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
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
            }
        }
    }

    private static void binaryTests(String namespace, String table, String family, Configuration conf) throws IOException {
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
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("\nColuna : " + Bytes.toString(column));
                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(), family_cell.getValueOffset() + family_cell.getValueLength());
                for (byte b : value) {
                    //System.out.println(String.valueOf(b));
                }
                boolean print = false;
                if (print) {
                    byte[] dunno = Arrays.copyOfRange(rowArray, 0,
                            4);
                    System.out.println("dunno: "
                            + Bytes.toInt(dunno) + " - 0 - 4");

                    byte[] row1 = Arrays.copyOfRange(rowArray, family_cell.getRowOffset(),
                            family_cell.getRowOffset() + family_cell.getRowLength());
                    System.out.println("row1: "
                            + Bytes.toString(row1) + " - " + family_cell.getRowOffset() + " - "
                            + (family_cell.getRowOffset() + family_cell.getRowLength()));

                    byte bt = rowArray[family_cell.getFamilyOffset() - 1];
                    System.out.println(bt + " - " + (family_cell.getFamilyOffset() - 1) + " - " + (family_cell.getFamilyOffset() - 1));

                    byte[] fam = Arrays.copyOfRange(rowArray, family_cell.getFamilyOffset(),
                            family_cell.getFamilyOffset() + family_cell.getFamilyLength());
                    System.out.println("fam : "
                            + Bytes.toString(fam) + " - " + family_cell.getFamilyOffset() + " - "
                            + (family_cell.getFamilyOffset() + family_cell.getFamilyLength()));

                    byte[] qualif = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(),
                            family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                    System.out.println("qualif : "
                            + Bytes.toString(qualif) + " - " + family_cell.getQualifierOffset()
                            + " - " + (family_cell.getQualifierOffset() + family_cell.getQualifierLength()));

                    /*byte[]  tag= Arrays.copyOfRange(rowArray, family_cell.getTagsOffset(), 
                        family_cell.getTagsOffset()+family_cell.getTagsLength());
                System.out.println("tag : " + 
                Bytes.toString(tag) +" - "+family_cell.getTagsOffset()+
                        " - "+(family_cell.getTagsOffset()+family_cell.getTagsLength()));
                     */
                    long typ = family_cell.getTimestamp();
                    byte[] typ1 = Arrays.copyOfRange(rowArray, (family_cell.getQualifierOffset() + family_cell.getQualifierLength()),
                            family_cell.getQualifierOffset() + family_cell.getQualifierLength() + 8);

                    byte[] toBytes = Bytes.toBytes(typ);
                    System.out.println("stamp : " + typ + " - "
                            + (family_cell.getQualifierOffset() + family_cell.getQualifierLength()) + " - "
                            + (family_cell.getQualifierOffset() + family_cell.getQualifierLength() + 8));

                    bt = rowArray[family_cell.getValueOffset() - 1];
                    System.out.println("type: " + bt + " - " + (family_cell.getValueOffset() - 1) + " - " + (family_cell.getValueOffset() - 1));

                }

                byte[] val = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                        family_cell.getValueOffset() + family_cell.getValueLength());
                System.out.println("value : "
                        + Bytes.toString(val) + " - " + family_cell.getValueOffset()
                        + " - " + (family_cell.getValueOffset() + family_cell.getValueLength()) + "\nlen " + family_cell.getValueLength());
                // do operation for DoubleF
                System.out.println("Valor : " + Bytes.toStringBinary(val));

                /* WEBTRASH
                String contentType = null;
                try {
                    contentType = URLConnection.guessContentTypeFromStream(
                            new ByteArrayInputStream(val));
                    System.out.println(contentType);
                } catch (IOException e) {
                    System.out.println("Could not guess content type");
                }

                contentType = new Tika().detect(val);
                System.out.println(contentType);
                 */
                for (int a = 0; a < rowArray.length; a++) {
                    //System.out.println("row : " +                             Bytes.toString(Arrays.copyOfRange(rowArray, a, a+1 ))+" - "+String.valueOf(Arrays.copyOfRange(rowArray, a, a+1 )[0]));
                }
                //System.out.println("row : " +                         Bytes.toString(Arrays.copyOfRange(rowArray, 0, 7 )));
                int a = 1;
                //System.out.println(35-26);
            }
        }
    }

    public static void dataTests(String namespace, String table, String family, Configuration conf) throws IOException {
        boolean bool1 = true, bool2 = false;
        char cha1 = 'a', cha2 = '9';
        short sho1 = 9;
        int in1 = 9, int2 = 57;
        long ln1 = 9;
        float ft1 = (float) 0.5;
        double db1 = 9.9; // long equal = 4621762822593629389
        String str1 = "9";
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        byte[] row1 = Bytes.toBytes("eduardo");
        Put p = new Put(row1);
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("charypselly"), Bytes.toBytes('ç'));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("str1"), Bytes.toBytes(str1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("bool1"), Bytes.toBytes(bool1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("cha1"), Bytes.toBytes(cha1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("cha2"), Bytes.toBytes(cha2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("sho1"), Bytes.toBytes(sho1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("in1"), Bytes.toBytes(in1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("in2"), Bytes.toBytes(int2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("bool2"), Bytes.toBytes(bool2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("ln1"), Bytes.toBytes(ln1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("ft1"), Bytes.toBytes(ft1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("db1"), Bytes.toBytes(db1));
        table2.put(p);
    }

    private static void typeTests(String namespace, String table, String family, Configuration conf) throws IOException {
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
                byte[] fam = Arrays.copyOfRange(rowArray, family_cell.getFamilyOffset(),
                        family_cell.getFamilyOffset() + family_cell.getFamilyLength());
                byte[] qua = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(),
                        family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("\ncoluna : " + Bytes.toString(fam) + ":" + Bytes.toString(qua));

                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                        family_cell.getValueOffset() + family_cell.getValueLength());;
                int len = family_cell.getValueLength();
                String valueOf = Bytes.toStringBinary(value);
                switch (len) {
                    case 1:
                        //bool or byte or string 
                        if (valueOf.contains("\\x")) {
                            if (valueOf.toLowerCase().contains("ff") || valueOf.toLowerCase().contains("00")) {
                                System.out.println(Bytes.toBoolean(value));
                            } else {
                                System.out.println(valueOf);
                            }
                        } else {
                            System.out.println(Bytes.toString(value));
                        }
                        break;
                    case 2:
                        if (valueOf.contains("\\x")) {
                            System.out.println(Bytes.toShort(value));
                        } else {
                            System.out.println(Bytes.toString(value));
                        }
                        break;
                    case 3:
                        //string
                        System.out.println(Bytes.toString(value));
                        break;
                    case 4:
                        //char or float or integer or string
                        if (valueOf.contains("\\x")) {
                            if (!valueOf.startsWith("\\x00\\x00\\x00\\x")&& valueOf.startsWith("\\x00\\x00\\x00")) {
                                System.out.println(Bytes.toString(value));
                            }
                        } else {
                            System.out.println(Bytes.toString(value));
                        }

                        System.out.println(Bytes.toFloat(value));
                        System.out.println(valueOf);
                        break;
                    case 5:
                    case 6:
                    case 7:
                        //string 
                        break;
                    case 8:
                        //double or long or string
                        break;
                    default:
                    //string or blob

                }

            }
        }
    }

}


/*
0-10 dunno

 */
