package com.num3rd.hbase.scheduler.put;

import com.num3rd.hbase.scheduler.ScheduledAuth;
import com.num3rd.hbase.utils.Contants;
import com.num3rd.hbase.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

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
        configuration.addResource(new Path(System.getenv(Contants.HADOOP_CONF_DIR), Contants.HDFS_SITE));
        configuration.set("fs.defaultFS", hdfsUrl);
        configuration.set("hadoop.security.authentication", "Kerberos");

        ScheduledExecutorService scheduledExecutorServiceAuth = Executors.newSingleThreadScheduledExecutor();
        ScheduledAuth scheduledAuth = new ScheduledAuth(k6sUser, k6sKeytab, configuration);
        scheduledAuth.run();
        scheduledExecutorServiceAuth.scheduleAtFixedRate(scheduledAuth, 3, 3, TimeUnit.HOURS);

        HbaseUtils.createSchemaTable(configuration);

        int corePoolSize = 3;
        ScheduledExecutorService scheduledExecutorServicePut = Executors.newScheduledThreadPool(corePoolSize);
        ScheduledPut scheduledPut = new ScheduledPut(configuration);
        for (int i = 0; i < corePoolSize; i++) {
            scheduledExecutorServicePut.scheduleAtFixedRate(scheduledPut, (i * 9 + i), 30, TimeUnit.SECONDS);
        }
    }
}
