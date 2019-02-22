package com.num3rd.hbase;

/**
 * Reference
 * https://hbase.apache.org/book.html#_examples
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HbaseKerberosDemo {
    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage:main k6sKeytab k6sUser hdfsUrl");
            System.exit(-1);
        }
        String k6sUser = args[0];
        String k6sKeytab = args[1];
        String hdfsUrl = args[2];

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        configuration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        configuration.set("fs.defaultFS", hdfsUrl);
        configuration.set("hadoop.security.authentication", "Kerberos");

        authKerberos(k6sUser, k6sKeytab, configuration);

        createSchemaTable(configuration);
        modifySchema(configuration);
    }

    private static void authKerberos(String k6sUser, String k6sKeytab, Configuration configuration) throws IOException {
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(k6sUser, k6sKeytab);
    }

    private static void modifySchema(Configuration configuration) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(TABLE_NAME);
        if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exists. ");
            System.exit(-1);
        }

        // Update exists table
        HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
        newColumn.setCompactionCompressionType(Algorithm.GZ);
        newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        admin.addColumn(tableName, newColumn);

        // Update exists column family
        HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
        existingColumn.setCompactionCompressionType(Algorithm.GZ);
        existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        /**
         * Does not work
         * IllegalArgumentException: Column family 'DEFAULT_COLUMN_FAMILY' does not exist
         *
         HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
         hTableDescriptor.modifyFamily(existingColumn);
         admin.modifyTable(tableName, hTableDescriptor);
         */
        admin.modifyColumn(tableName, existingColumn);

        // Disable an existing table
        admin.disableTable(tableName);

        // Delete an existing column family
        admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

        // Delete a table (Need to be disabled first)
        admin.deleteTable(tableName);
    }

    private static void createSchemaTable(Configuration configuration) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        hTableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

        System.out.println("Create table. ");
        createOrOverwrite(admin, hTableDescriptor);
        System.out.println(" Done. ");

        // Put one row
        String currentTimeMillis = String.valueOf(System.currentTimeMillis());
        String reverseCurrentTimeMillis = new StringBuffer(currentTimeMillis).reverse().toString();
        Put put = new Put(Bytes.toBytes(reverseCurrentTimeMillis.concat("-r")));
        put.addColumn(
                Bytes.toBytes(CF_DEFAULT),
                Bytes.toBytes(currentTimeMillis.concat("-q")),
                Bytes.toBytes(currentTimeMillis.concat("-v"))
        );
        table.put(put);
    }

    private static void createOrOverwrite(Admin admin, HTableDescriptor hTableDescriptor) throws IOException {
        if (admin.tableExists(hTableDescriptor.getTableName())) {
            admin.disableTable(hTableDescriptor.getTableName());
            admin.deleteTable(hTableDescriptor.getTableName());
        }
        admin.createTable(hTableDescriptor);
    }
}
