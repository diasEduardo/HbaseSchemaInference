/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HbaseSchemaInference.model;

import HbaseSchemaInference.control.HbaseOperations;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class RawSchema {
    
    private String namespace, newNamespace;
    private final String rowName = "raw";
    HbaseOperations ops;
    private final int byteNum = 1, booleanNum = 2, stringNum = 4, shortNum = 8,
            charNum = 16, floatNum = 32, integerNum = 64, doubleNum = 1,
            longNum = 2, blobNum = 4;
    
    public RawSchema(String namespace, HbaseOperations ops, String newNamespace) {
        this.namespace = namespace;
        this.ops = ops;
        this.newNamespace = newNamespace;
    }
    
    public long getRawSchema() {
        Date date = new Date();
        short createNamespace = ops.createNamespace(newNamespace);
        if (createNamespace < 0) {
            return -1;
        } else if (createNamespace == 1) {
            String[] tables = ops.getTables(newNamespace);
            for (String table : tables) {
                ops.deleteTable(namespace, table);
            }
        }
        ArrayList<PutData> data = new ArrayList<PutData>();
        String[] tables = ops.getTables(namespace);
        for (String table : tables) {
            String[] families = ops.getFamilies(namespace, table);
            ops.createTable(newNamespace, table, families);
            for (String family : families) {
                byte[] fam = Bytes.toBytes(family);
                String[] columns = ops.getColumns(namespace, table, family);
                for (String column : columns) {
                    byte[] col = Bytes.toBytes(column);
                    byte[] value = columnAnalysis(namespace, table, family, column);
                    //ops.putData(newNamespace, table, rowName, fam, col, value);
                    data.add(new PutData(fam, col, value));
                }
            }
            ops.putArrayOfData(newNamespace, table, rowName, data);
        }
        Date date2 = new Date();
        return date2.getTime() - date.getTime();
    }
    
    private byte[] columnAnalysis(String namespace, String table, String family, String column) {
        byte[] type = new byte[]{0x7, (byte) 0xFF};
        boolean fixedLenght = true;
        int lenght = 0;
        byte[] output = new byte[7];
        try {
            ResultScanner scanner = ops.getValuesScan(namespace, table, family, column);
            
            for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
                List<Cell> family_cells = result2.listCells();
                
                for (Cell family_cell : result2.listCells()) {
                    byte[] rowArray = family_cell.getRowArray();
                    int valueLen = family_cell.getValueLength();
                    if (lenght == 0) {
                        lenght = valueLen;
                    }
                    if (fixedLenght && lenght != valueLen) {
                        fixedLenght = false;
                    }
                    
                    byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                            family_cell.getValueOffset() + family_cell.getValueLength());
                    byte[] types = getTypes(valueLen, value);
                    type[0] &= types[0];
                    type[1] &= types[1];
                }
            }
            scanner.close();
        } catch (IOException ex) {
            return null;
        }
        
        byte[] fLen = Bytes.toBytes(fixedLenght);
        output[0] = fLen[0];
        byte[] len = Bytes.toBytes(lenght);
        output[1] = len[0];
        output[2] = len[1];
        output[3] = len[2];
        output[4] = len[3];
        output[5] = type[0];
        output[6] = type[1];
        return output;
    }
    
    private byte[] getTypes(int len, byte[] value) {
        byte[] output = new byte[]{0x0, 0x0};
        switch (len) {
            case 1:
                if (isUtf8Valid(value)) {
                    //string or byte
                    output[1] |= (byteNum + stringNum);
                    
                } else if (value[0] == -1 || value[0] == 0) {
                    //boolean or byte
                    output[1] |= (byteNum + booleanNum);
                    
                } else {
                    //byte
                    output[1] |= byteNum;
                }
                break;
            case 2:
                if (isUtf8Valid(value)) {
                    //string or short
                    output[1] |= (shortNum + stringNum);
                } else {
                    //short
                    output[1] |= shortNum;
                }
                break;
            case 3:
                //string
                if (isUtf8Valid(value)) {
                    output[1] |= stringNum;
                } else {
                    output[0] |= blobNum;
                }
                break;
            case 4:
                //char or float or integer or string

                if (isUtf8Valid(value)) {
                    output[1] |= stringNum;
                } else if (value[0] == 0 && value[1] == 0 && (value[2] != 0 || value[3] != 0)) {
                    output[1] |= charNum;
                }
                
                if ((value[0] != -1 && value[0] != 127) || value[1] >= 0) {
                    output[1] |= floatNum;
                }
                
                output[1] |= integerNum;
                break;
            case 5:
            case 6:
            case 7:
                //string
                if (isUtf8Valid(value)) {
                    output[1] |= stringNum;
                } else {
                    output[0] |= blobNum;
                }
                break;
            case 8:
                //double or long or string
                if (isUtf8Valid(value)) {
                    output[1] |= stringNum;
                }
                //01111111 11110000
                if ((value[0] != -1 && value[0] != 127) || (value[1] < -16 || value[1] > -1)) {
                    output[0] |= doubleNum;
                }
                
                output[0] |= longNum;
                break;
            default:
                //string or blob
                if (isUtf8Valid(value)) {
                    output[1] |= stringNum;
                } else {
                    output[0] |= blobNum;
                }
            
        }
        return output;
        
    }
    
    private boolean isUtf8Valid(byte[] value) {
        int c1Min = 0x1, c1Max = 0x7E;
        int c2Min = 0xC2, c2Max = 0xDF;
        int c3Min = 0xE0, c3Max = 0xEF;
        int c4Min = 0xF0, c4Max = 0xF5;
        int min = 0x80, max = 0xBF;        
        
        for (int i = 0; i < value.length; i++) {
            int val = byteToIntUnsigned(value[i]);
            
            if ((int) value[i] >= c1Min && val <= c1Max) {
                continue;
            } else if ((i + 1) < value.length && val >= c2Min &&  val <= c2Max) {
                int val1 = byteToIntUnsigned(value[i + 1]);
                if (val1 >= min && val1 <= max) {
                    i++;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 2) < value.length && val >= c3Min && val <= c3Max) {
                int val1 = byteToIntUnsigned(value[i + 1]);
                int val2 = byteToIntUnsigned(value[i + 2]);
                if (val1 >= min && val1 <= max
                        && val2 >= min && val2 <= max) {
                    i += 2;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 3) < value.length && val >= c4Min && val <= c4Max) {
                int val1 = byteToIntUnsigned(value[i + 1]);
                int val2 = byteToIntUnsigned(value[i + 2]);
                int val3 = byteToIntUnsigned(value[i + 3]);
                if (val1 >= min && val1 <= max
                        && val2 >= min && val2 <= max
                        && val3 >= min && val3 <= max) {
                    i += 3;
                    continue;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
    
    public long rawToJSON() {
        Date date1 = new Date();
        String[] tables = ops.getTables(newNamespace);
        for (String table : tables) {
            try {
                ResultScanner scanner = ops.getTableScan(newNamespace, table);
                for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
                    List<Cell> family_cells = result2.listCells();
                    
                    for (Cell family_cell : result2.listCells()) {
                        byte[] rowArray = family_cell.getRowArray();
                        
                        byte[] col = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(),
                                family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                        System.out.print("\n\n" + Bytes.toString(col) + " - ");
                        byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                                family_cell.getValueOffset() + family_cell.getValueLength());
                        
                        byte[] type = Arrays.copyOfRange(value, 5, 7);
                        boolean fixedLenght = Bytes.toBoolean(Arrays.copyOfRange(value, 0, 1));
                        int lenght = Bytes.toInt(Arrays.copyOfRange(value, 1, 5));
                        if (!fixedLenght) {
                        }
                        if ((type[1] & byteNum) != 0) {
                            System.out.print("byte, ");
                        }
                        if ((type[1] & booleanNum) != 0) {
                            System.out.print("boolean, ");
                        }
                        if ((type[1] & stringNum) != 0) {
                            System.out.print("string, ");
                        }
                        if ((type[1] & shortNum) != 0) {
                            System.out.print("short, ");
                        }
                        if ((type[1] & charNum) != 0) {
                            System.out.print("char, ");
                        }
                        if ((type[1] & floatNum) != 0) {
                            System.out.print("float, ");
                        }
                        if ((type[1] & integerNum) != 0) {
                            System.out.print("Integer, ");
                        }
                        if ((type[0] & doubleNum) != 0) {
                            System.out.print("double, ");
                        }
                        if ((type[0] & longNum) != 0) {
                            System.out.print("long, ");
                        }
                        if ((type[0] & blobNum) != 0) {
                            System.out.print("blob ");
                        }
                        
                    }
                }
                scanner.close();
            } catch (IOException ex) {
                Logger.getLogger(RawSchema.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Date date2 = new Date();
        return date2.getTime() - date1.getTime();
        
    }
    
    private int byteToIntUnsigned(byte value) {
        byte[] result = new byte[]{0x0, 0x0, 0x0, 0x0};
        result[3] |= value;
        return Bytes.toInt(result);
    }
    
}
