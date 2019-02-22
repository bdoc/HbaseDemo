package com.num3rd.hbase.scheduler;

import com.num3rd.hbase.utils.Contants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledMain {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage:main k6sKeytab k6sUser hdfsUrl");
            System.exit(-1);
        }
        String k6sUser = args[0];
        String k6sKeytab = args[1];
        String hdfsUrl = args[2];

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path(System.getenv(Contants.HBASE_CONF_DIR), Contants.HBASE_SITE));
        configuration.addResource(new Path(System.getenv(Contants.HADOOP_CONF_DIR), Contants.CORE_SITE));
        configuration.set("fs.defaultFS", hdfsUrl);
        configuration.set("hadoop.security.authentication", "Kerberos");

        int corePoolSize = 1;
        ScheduledExecutorService scheduledExecutorServiceAuth = Executors.newScheduledThreadPool(corePoolSize);
        ScheduledAuth scheduledAuth = new ScheduledAuth(k6sUser, k6sKeytab, configuration);
        scheduledAuth.run();
        scheduledExecutorServiceAuth.scheduleAtFixedRate(scheduledAuth, 3, 3, TimeUnit.HOURS);

        createSchemaTable(configuration);

        corePoolSize = 3;
        ScheduledExecutorService scheduledExecutorServicePut = Executors.newScheduledThreadPool(corePoolSize);
        ScheduledPut scheduledPut = new ScheduledPut(configuration);
        for (int i = 0; i < corePoolSize; i++) {
            scheduledExecutorServicePut.scheduleAtFixedRate(scheduledPut, (i * 9 + i), 30, TimeUnit.SECONDS);
        }
    }

    private static void createSchemaTable(Configuration configuration) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(Contants.TABLE_NAME));
        hTableDescriptor.addFamily(new HColumnDescriptor(Contants.CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));

        System.out.println("Create table. ");
        createOrOverwrite(admin, hTableDescriptor);
        System.out.println(" Done. ");
    }

    private static void createOrOverwrite(Admin admin, HTableDescriptor hTableDescriptor) throws IOException {
        if (admin.tableExists(hTableDescriptor.getTableName())) {
            admin.disableTable(hTableDescriptor.getTableName());
            admin.deleteTable(hTableDescriptor.getTableName());
        }
        admin.createTable(hTableDescriptor);
    }
}
