package com.num3rd.hbase.scheduler.get;

import com.num3rd.hbase.utils.Contants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.logging.Logger;

public class ScheduledGet implements Runnable {
    Logger LOG = Logger.getLogger(getClass().getName());
    private Configuration configuration;
    private Get get;

    public ScheduledGet(Configuration configuration, Get get) {
        this.configuration = configuration;
        this.get = get;
    }

    public void run() {
        LOG.info(getClass().getName());
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);

            Table table = connection.getTable(TableName.valueOf(Contants.TABLE_NAME));

            Result  result = table.get(get);

            System.out.println(Bytes.toString(result.value()));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
