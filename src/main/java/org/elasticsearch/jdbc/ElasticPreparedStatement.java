package org.elasticsearch.jdbc;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.utils.Constants;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ElasticPreparedStatement extends AbstractFeatureNotSupportedPreparedStatement {
    private Map<Integer, SQLParam> paramMap = Maps.newHashMap();

    private String sql;

    public ElasticPreparedStatement(ElasticConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();
        return true;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (paramMap.size() > 0) {
            List<SQLParam> paramList = Lists.newArrayList(paramMap.values());
            Collections.sort(paramList, new Comparator<SQLParam>() {
                @Override
                public int compare(SQLParam o1, SQLParam o2) {
                    if (o1.getParamIndex() < o2.getParamIndex()) {
                        return -1;
                    }
                    if (o1.getParamIndex() > o2.getParamIndex()) {
                        return 1;
                    }
                    return 0;
                }
            });

            List<Object> argList = Lists.transform(paramList, new Function<SQLParam, Object>() {
                @Override
                public Object apply(SQLParam sqlParam) {
                    return sqlParam.getParamVal();
                }
            });

            return executeQuery(sql, argList.toArray(new Object[argList.size()]));
        }
        return executeQuery(sql);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DEFAULT_ES_DATE_FORMAT);
        String dateStr = dateFormat.format(x);
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, dateStr));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DEFAULT_ES_DATE_FORMAT);
        String dateStr = dateFormat.format(x);
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, dateStr));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DEFAULT_ES_DATE_FORMAT);
        String dateStr = dateFormat.format(x);
        paramMap.put(parameterIndex, new SQLParam(parameterIndex, dateStr));
    }

    @Override
    public void clearParameters() throws SQLException {
        paramMap.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        }
        else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        }
        else if (x instanceof java.util.Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DEFAULT_ES_DATE_FORMAT);
            String dateStr = dateFormat.format(x);
            paramMap.put(parameterIndex, new SQLParam(parameterIndex, dateStr));
        }
        else {
            paramMap.put(parameterIndex, new SQLParam(parameterIndex, x));
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ElasticResultSetMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    private class SQLParam {

        private int paramIndex;

        private Object paramVal;

        public SQLParam(int paramIndex, Object paramVal) {
            this.paramIndex = paramIndex;
            this.paramVal = paramVal;
        }

        public Object getParamVal() {
            return paramVal;
        }

        public int getParamIndex() {
            return paramIndex;
        }

    }
}
