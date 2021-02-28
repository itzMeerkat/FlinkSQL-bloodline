package me.jiayizhang.sqlparser;

public class ConnectorMetaData {
    public TableType sinkOrSource;
    public String Name;

    public String sqlCallType;

    public String Type;
    public String Topic;
    public String GroupId;
    public String Tag;
    public String FormatType;
    public String ESIndex;
    public String DorisDBName;
    public String TableName;
    public String Cluster;

    public ConnectorMetaData() {
        sinkOrSource = TableType.Source;
    }

    public void parseArgument(String key, String value) {
        switch (key) {
            case "connector.type" -> this.Type = value;
            case "connector.topic", "connector.consumer.topic" -> this.Topic = value;
            case "connector.group.id", "connector.consumer.group", "connector.producer.group" -> this.GroupId = value;
            case "connector.consumer.tag", "connector.producer.tag" -> this.Tag = value;
            case "format.type", "connector.data-format" -> this.FormatType = value;
            case "connector.index" -> this.ESIndex = value;
            case "connector.db-name" -> this.DorisDBName = value;
            case "connector.table-name", "connector.table", "format.target-table" -> this.TableName = value;
            case "connector.cluster" -> this.Cluster = value;
//            default -> System.out.printf("Ignored k-v pair: %s -> %s\n", key,value);
        }
    }

    public String Describe() {
        return String.join(", ", Name, sinkOrSource.name(),Type, Topic, GroupId, Tag, FormatType,ESIndex);
    }

    public String MakeKey() {
        String res=null;
        switch (this.Type)  {
            case "kafka" -> res = String.join("::", this.Type, this.Cluster, this.Topic);
            case "abase" -> res = String.join("::", this.Type, this.Cluster, this.TableName);
            case "print" -> res = "print";
            case "elasticsearch" -> res = String.join("::", this.Type, this.ESIndex);
            case "doris" -> res = String.join("::", this.Type, this.Cluster, this.DorisDBName, this.TableName);
            case "redis" -> res = String.join("::", this.Type, this.Cluster);
            case "rocketmq" -> res = String.join("::", this.Type, this.Topic, this.Tag);
            default -> res = String.format("Missing type %s", this.Type);
        }
        return res;
    }
}
