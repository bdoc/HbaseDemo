package com.num3rd.hbase.scheduler;

import com.num3rd.hbase.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.logging.Logger;

public class ScheduledAuth implements Runnable {
    Logger LOG = Logger.getLogger(getClass().getName());
    private String k6sUser;
    private String k6sKeytab;
    private Configuration configuration;

    public ScheduledAuth(String k6sUser, String k6sKeytab, Configuration configuration) {
        this.k6sUser = k6sUser;
        this.k6sKeytab = k6sKeytab;
        this.configuration = configuration;
    }

    public void run() {
        LOG.info(getClass().getName());
        try {
            HbaseUtils.authKerberos(k6sUser, k6sKeytab, configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
