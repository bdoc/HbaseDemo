package com.num3rd.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HbaseUtils {
    public static void authKerberos(String k6sUser, String k6sKeytab, Configuration configuration) throws IOException {
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(k6sUser, k6sKeytab);
    }

    public static void createSchemaTable(Configuration configuration) throws IOException {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(Contants.TABLE_NAME));
            hTableDescriptor.addFamily(new HColumnDescriptor(Contants.CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));

            System.out.println("Create table. ");
            createOrOverwrite(admin, hTableDescriptor);
            System.out.println(" Done. ");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    private static void createOrOverwrite(Admin admin, HTableDescriptor hTableDescriptor) throws IOException {
        if (admin.tableExists(hTableDescriptor.getTableName())) {
            admin.disableTable(hTableDescriptor.getTableName());
            admin.deleteTable(hTableDescriptor.getTableName());
        }
        admin.createTable(hTableDescriptor);
    }
}
