package v10.com.alibaba.druid.pool;

import v10.com.alibaba.druid.util.jdbc.ResultSetBase;
import v10.com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;

import java.util.List;

/**
 * Created by allwefantasy on 8/31/16.
 */
public class ElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {

    public ElasticSearchResultSetMetaDataBase(List<String> headers) {
        ColumnMetaData columnMetaData;
        for (String column : headers) {
            columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            getColumns().add(columnMetaData);
        }
    }

}
