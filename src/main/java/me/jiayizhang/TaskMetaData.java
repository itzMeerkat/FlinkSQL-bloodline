package me.jiayizhang;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TaskMetaData {
    public final HashMap<String, ConnectorMetaData> map = new HashMap<>();
    public String Name;
    TaskMetaData(String n) {
        Name = n;
    }

    public void NewSqlCall(ConnectorMetaData meta) {
        if(meta.sqlCallType == null)
            return;

        if(meta.sqlCallType.equals("insert")) {
            this.SetSink(meta.Name);
        } else if (meta.sqlCallType.equals("create_table")) {
            System.out.printf("New table created: %s\n", meta.Name);
            map.put(meta.Name, meta);
        }
    }

    private boolean SetSink(String name) {
        ConnectorMetaData getMeta;
        try {
            getMeta = map.get(name);
            getMeta.sinkOrSource = TableType.Sink;
            map.put(name, getMeta);
            return true;
        } catch (NullPointerException e) {
            System.out.printf("Can't find SqlCall with name %s\n", name);
        }
        return false;
    }

    public ArrayList<ConnectorMetaData> GetSources() {
        ArrayList<ConnectorMetaData> ret = new ArrayList<>();
        for(Map.Entry<String, ConnectorMetaData> e: map.entrySet()) {
            if(e.getValue().sinkOrSource == TableType.Source) {
                ret.add(e.getValue());
            }
        }
        return ret;
    }

    public ArrayList<ConnectorMetaData> GetSinks() {
        ArrayList<ConnectorMetaData> ret = new ArrayList<>();
        for(Map.Entry<String, ConnectorMetaData> e: map.entrySet()) {
            if(e.getValue().sinkOrSource == TableType.Sink) {
                ret.add(e.getValue());
            }
        }
        return ret;
    }
}
