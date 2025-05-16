package dev.mwiater.postgrestablecompare;

import lombok.Data;

@Data
public class Compare {

    public String sourceTable;
    public String targetTable;
    public String sourceSchema;
    public String targetSchema;

    public String getInternalTableName() {
        return sourceTable + "_" + targetTable;
    }
}
