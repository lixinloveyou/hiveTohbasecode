package com.hdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public class Step2 {

    public void goTo() throws IOException {
        Configuration hbaseconfiguration = HBaseConfiguration.create();
        Connection connection = null;
        connection =  ConnectionFactory.createConnection(hbaseconfiguration);
        HTable htable = (HTable) connection.getTable(TableName.valueOf("broadbanduseridtophonenumber"));
    }
}
