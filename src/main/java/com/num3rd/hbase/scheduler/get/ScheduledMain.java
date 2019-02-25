package com.num3rd.hbase.scheduler.get;

import com.num3rd.hbase.scheduler.ScheduledAuth;
import com.num3rd.hbase.utils.Contants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

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

        ScheduledExecutorService scheduledExecutorServiceAuth = Executors.newSingleThreadScheduledExecutor();
        ScheduledAuth scheduledAuth = new ScheduledAuth(k6sUser, k6sKeytab, configuration);
        scheduledAuth.run();
        scheduledExecutorServiceAuth.scheduleAtFixedRate(scheduledAuth, 3, 3, TimeUnit.HOURS);

        int corePoolSize = 3;
        ScheduledExecutorService scheduledExecutorServicePut = Executors.newScheduledThreadPool(corePoolSize);
        ScheduledGet scheduledGet = new ScheduledGet(configuration, put(configuration));
        for (int i = 0; i < corePoolSize; i++) {
            scheduledExecutorServicePut.scheduleAtFixedRate(scheduledGet, (i * 9 + i), 30, TimeUnit.SECONDS);
        }
    }

    private static Get put(Configuration configuration) throws IOException {
        String currentTimeMillis = String.valueOf(System.currentTimeMillis());
        String reverseCurrentTimeMillis = new StringBuffer(currentTimeMillis).reverse().toString();

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);

            Table table = connection.getTable(TableName.valueOf(Contants.TABLE_NAME));

            Put put = new Put(Bytes.toBytes(reverseCurrentTimeMillis.concat("-r")));
            put.addColumn(
                    Bytes.toBytes(Contants.CF_DEFAULT),
                    Bytes.toBytes(currentTimeMillis.concat("-q")),
                    Bytes.toBytes(currentTimeMillis.concat("-v"))
            );

            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }

        return new Get(Bytes.toBytes(reverseCurrentTimeMillis.concat("-r")));
    }

}
