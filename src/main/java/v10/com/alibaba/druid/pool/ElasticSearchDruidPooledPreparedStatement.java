package v10.com.alibaba.druid.pool;

import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.ESActionFactory;

import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;
import v10.com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import v10.com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import v10.com.alibaba.druid.support.logging.Log;
import v10.com.alibaba.druid.support.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {
    private final static Log LOG = LogFactory.getLog(ElasticSearchDruidPooledPreparedStatement.class);
    private final Client client;

    public ElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder) throws SQLException {
        super(conn, holder);
        this.client = ((ElasticSearchConnection) conn.getConnection()).getClient();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();

        incrementExecuteCount();
        transactionRecord(getSql());

        oracleSetRowPrefetch();

        conn.beforeExecute();
        try {
            String executionSql = getSql();
            ObjectResult extractor = getObjectResult(true, executionSql, false, false, true);
            List<String> headers = extractor.getHeaders();
            List<List<Object>> lines = extractor.getLines();
            additionalMissingField(headers, executionSql);
            ResultSet rs = new ElasticSearchResultSet(this, headers, lines);

            if (rs == null) {
                return null;
            }

            DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);

            return poolableResultSet;
        } catch (Throwable t) {
            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }

    private void additionalMissingField(List<String> headers, String executionSql) throws SqlParseException, SQLException {
        SQLQueryExpr sqlExpr = (SQLQueryExpr) ESActionFactory.toSqlExpr(executionSql);
        if(sqlExpr.getSubQuery().getQuery() instanceof SQLUnionQuery){
            return;
        }

       if(executionSql.contains("join")){
           return;
       }

        Select select = new SqlParser().parseSelect(sqlExpr);
        List<Field> fields = select.getFields();
        for (Field field : fields) {
            if (field.getAlias() == null) {
                if (!headers.contains(field.getName())) {
                    headers.add(field.getName());
                }
            } else {
                if (!headers.contains(field.getAlias())) {
                    headers.add(field.getAlias());
                }
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();

        incrementExecuteCount();
        transactionRecord(getSql());

        // oracleSetRowPrefetch();

        conn.beforeExecute();
        try {
            ObjectResult extractor = getObjectResult(true, getSql(), false, false, true);
            List<String> headers = extractor.getHeaders();
            List<List<Object>> lines = extractor.getLines();

            ResultSet rs = new ElasticSearchResultSet(this, headers, lines);
            ((ElasticSearchPreparedStatement) getRawPreparedStatement()).setResults(rs);

            return true;
        } catch (Throwable t) {
            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }

    private ObjectResult getObjectResult(boolean flat, String query, boolean includeScore, boolean includeType, boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);

        //String rewriteSQL = searchDao.explain(getSql()).explain().explain();

        QueryAction queryAction = searchDao.explain(query);
        LOG.info("Execute the SQL parsed statement is " + queryAction.explain().explain());
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new ObjectResultsExtractor(includeScore, includeType, includeId).extractResults(execution, flat);
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("executeUpdate not support in ElasticSearch");
    }
}
