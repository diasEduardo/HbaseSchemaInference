/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HbaseSchemaInference.model;

import HbaseSchemaInference.control.HbaseOperations;
import java.util.Date;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class RawSchema {

    private String namespace;
    HbaseOperations ops;
    private final int byteNum = 1, booleanNum = 2, stringNum = 4, shortNum = 8,
            charNum = 16, floatNum = 32, integerNum = 64, doubleNum = 1,
            longNum = 2, blobNum = 4;

    public RawSchema(String namespace, HbaseOperations ops) {
        this.namespace = namespace;
        this.ops = ops;
    }

    public short getRawSchema() {
        Date date = new Date();
        String newNamespace = namespace + "_rawSchema_" + date.getTime();
        short createNamespace = ops.createNamespace(newNamespace);
        if (createNamespace < 0) {
            return -1;
        } else if (createNamespace == 1) {
            String[] tables = ops.getTables(newNamespace);
            for (String table : tables) {
                ops.deleteTable(namespace, table);
            }
        }

        String[] tables = ops.getTables(namespace);
        for (String table : tables) {
            String[] families = ops.getfamilies(namespace, table);
            ops.createTable(newNamespace, table, families);
        }

        return 0;
    }

    private byte[] getTypes(int len, byte[] value) {
        byte[] output = new byte[]{0x0, 0x0};
        switch (len) {
            case 1:
                if (isUtf8Valid(value)) {
                    //string or byte
                    output[1] |= 5;

                } else if (value[0] == -1 || value[0] == 0) {
                    //boolean or byte
                    output[1] |= 3;

                } else {
                    //byte
                    output[1] |= 1;
                }
                break;
            case 2:
                if (isUtf8Valid(value)) {
                    //string or short
                    output[1] |= 12;
                } else {
                    //short
                    output[1] |= 8;
                }
                break;
            case 3:
                //string
                if (isUtf8Valid(value)) {
                    output[1] |= 4;
                } else {
                    output[0] |= 4;
                }
                break;
            case 4:
                //char or float or integer or string

                if (isUtf8Valid(value)) {
                    output[1] |= 4;
                } else if (value[0] == 0 && value[1] == 0 && (value[2] != 0 || value[3] != 0)) {
                    output[1] |= 16;
                }

                if ((value[0] != -1 && value[0] != 127) || value[1] >= 0) {
                    output[1] |= 32;
                }

                output[1] |= 64;
                break;
            case 5:
            case 6:
            case 7:
                //string
                if (isUtf8Valid(value)) {
                    output[1] |= 4;
                } else {
                    output[0] |= 4;
                }
                break;
            case 8:
                //double or long or string
                if (isUtf8Valid(value)) {
                    output[1] |= 4;
                }
                //01111111 11110000
                if ((value[0] != -1 && value[0] != 127) || (value[1] < -16 || value[1] > -1)) {
                    output[0] |= 1;
                }

                output[0] |= 2;
                break;
            default:
                //string or blob
                if (isUtf8Valid(value)) {
                    output[1] |= 4;
                } else {
                    output[0] |= 4;
                }

        }
        return output;

    }

    private boolean isUtf8Valid(byte[] value) {
        int c1Min = 0x1, c1Max = 0x7E;
        int c2Min = 0xC0, c2Max = 0xDF;
        int c3Min = 0xE0, c3Max = 0xEF;
        int c4Min = 0xF0, c4Max = 0xF7;
        int min = 0x80, max = 0xBF;

        for (int i = 0; i < value.length; i++) {
            if (value[i] >= c1Min && value[i] <= c1Max) {
                continue;
            } else if ((i + 1) < value.length && value[i] >= c2Min && value[i] <= c2Max) {

                if (value[i + 1] >= min && value[i + 1] <= max) {
                    i++;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 2) < value.length && value[i] >= c3Min && value[i] <= c3Max) {

                if (value[i + 1] >= min && value[i + 1] <= max
                        && value[i + 2] >= min && value[i + 2] <= max) {
                    i += 2;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 3) < value.length && value[i] >= c4Min && value[i] <= c4Max) {

                if (value[i + 1] >= min && value[i + 1] <= max
                        && value[i + 2] >= min && value[i + 2] <= max
                        && value[i + 3] >= min && value[i + 3] <= max) {
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

}
