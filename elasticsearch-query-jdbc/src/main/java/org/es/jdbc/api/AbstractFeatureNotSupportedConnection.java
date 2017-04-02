package org.es.jdbc.api;


import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public abstract class AbstractFeatureNotSupportedConnection implements Connection {

    @Override
    public final CallableStatement prepareCall(final String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public final CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public final CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public final String nativeSQL(final String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("nativeSQL");
    }

    @Override
    public final Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override
    public final Savepoint setSavepoint(final String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint name");
    }

    @Override
    public final void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");
    }

    @Override
    public final void rollback(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback savepoint");
    }

    @Override
    public final void abort(final Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("abort");
    }

    @Override
    public final String getCatalog() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalog");
    }

    @Override
    public final void setCatalog(final String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCatalog");
    }

    @Override
    public final String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchema");
    }

    @Override
    public final void setSchema(final String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSchema");
    }

    @Override
    public final Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap");
    }

    @Override
    public final void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    @Override
    public final int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout");
    }

    @Override
    public final void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout");
    }

    @Override
    public final Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    @Override
    public final Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    @Override
    public final NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override
    public final SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }

    @Override
    public final Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override
    public final Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override
    public final boolean isValid(final int timeout) throws SQLException {
        throw new SQLFeatureNotSupportedException("isValid");
    }

    @Override
    public final Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    @Override
    public final String getClientInfo(final String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo name");
    }

    @Override
    public final void setClientInfo(final String name, final String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("setClientInfo name value");
    }

    @Override
    public final void setClientInfo(final Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("setClientInfo properties");
    }
}
