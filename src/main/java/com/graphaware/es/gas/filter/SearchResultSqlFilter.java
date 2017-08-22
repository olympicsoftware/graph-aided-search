/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */


 /*
    Olympic Software - SQL Assisted Search
            Proof of Concept
 */
package com.graphaware.es.gas.filter;

import com.graphaware.es.gas.annotation.SearchFilter;
import com.graphaware.es.gas.cypher.CypherResult;
import com.graphaware.es.gas.cypher.CypherSettingsReader;
import com.graphaware.es.gas.cypher.ResultRow;
import com.graphaware.es.gas.domain.IndexInfo;
import com.graphaware.es.gas.util.NumberUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.*;
import java.sql.SQLException;

import static com.graphaware.es.gas.domain.ClauseConstants.*;
import static com.graphaware.es.gas.wrap.GraphAidedSearchActionListenerWrapper.GAS_FILTER_CLAUSE;

@SearchFilter(name = "SearchResultSqlFilter")
public class SearchResultSqlFilter extends CypherSettingsReader implements SearchResultFilter {

    private static final Logger logger = Logger.getLogger(SearchResultSqlFilter.class.getName());
    private final ESLogger esLogger;

    private static final String DEFAULT_ID_RESULT_NAME = "id";
    private static final String ID_RESULT_NAME_KEY = "identifier";
    private static final int DEFAULT_RESULT_SIZE = 10;
    private static final int DEFAULT_FROM_VALUE = 0;

    private int maxResultSize = -1;
    private int size;
    private int from;
    private boolean shouldExclude = true;
    private String idResultName;
    private String sqlQuery;
    private String sqlConnectionString;

    public SearchResultSqlFilter(Settings settings, IndexInfo indexSettings) {
        super(settings, indexSettings);
        this.esLogger = Loggers.getLogger("SearchResultSqlFilter", settings);
    }

    @Override
    public void parseRequest(Map<String, Object> sourceAsMap) {
        size = NumberUtil.getInt(sourceAsMap.get(SIZE), DEFAULT_RESULT_SIZE);
        from = NumberUtil.getInt(sourceAsMap.get(FROM), DEFAULT_FROM_VALUE);

        HashMap extParams = (HashMap) sourceAsMap.get(GAS_FILTER_CLAUSE);
        if (extParams != null) {
            sqlQuery = (String) extParams.get(QUERY);
            sqlConnectionString = (String) extParams.get(CONNECTION_STRING);
            maxResultSize = NumberUtil.getInt(extParams.get(MAX_RESULT_SIZE), getMaxResultWindow());
            shouldExclude = extParams.containsKey(EXCLUDE) && String.valueOf(extParams.get(EXCLUDE)).equalsIgnoreCase(TRUE);
            idResultName = extParams.containsKey(ID_RESULT_NAME_KEY) ? String.valueOf(extParams.get(ID_RESULT_NAME_KEY)) : null;
        }
        if (maxResultSize > 0) {
            sourceAsMap.put(SIZE, maxResultSize);
        }
        if (null == sqlQuery) {
            throw new RuntimeException("The query Parameter is required in gas-filter");
        }
        if (null == sqlConnectionString) {
            throw new RuntimeException("The connectionString Parameter is required in gas-filter");
        }
        esLogger.debug("sqlConnectionString:" + sqlConnectionString);
        esLogger.debug("sqlQuery:" + sqlQuery);
    }

    @Override
    public InternalSearchHits modify(final InternalSearchHits hits) {
        final InternalSearchHit[] searchHits = hits.internalHits();
        Map<String, InternalSearchHit> hitMap = new HashMap<>();
        for (InternalSearchHit hit : searchHits) {
            hitMap.put(hit.getId(), hit);
        }

        Set<String> remoteFilter = getFilteredItems();
        esLogger.debug("InternalSearchHits - remoteFilter.size():" + remoteFilter.size());
        esLogger.debug("InternalSearchHits - remoteFilter:" + remoteFilter);
        esLogger.debug("InternalSearchHits - shouldExclude:" + shouldExclude);

        InternalSearchHit[] tmpSearchHits = new InternalSearchHit[hitMap.size()];
        int k = 0;
        float maxScore = -1;
        for (Map.Entry<String, InternalSearchHit> item : hitMap.entrySet()) {
                esLogger.debug("InternalSearchHits - item.getKey:" + item.getKey());
                esLogger.debug("InternalSearchHits - remoteFilter.contains(item.getKey()):" + remoteFilter.contains(item.getKey()));
                if ((shouldExclude && !remoteFilter.contains(item.getKey()))
                    || (!shouldExclude && remoteFilter.contains(item.getKey()))) {
                                esLogger.debug("InternalSearchHits - item will be included in result set");
                                tmpSearchHits[k] = item.getValue();
                                k++;
                                float score = item.getValue().getScore();
                                if (maxScore < score) {
                                    maxScore = score;
                                }
                }
                else
                {
                        esLogger.debug("InternalSearchHits - item will *NOT* be included in result set");
                }
        }
        int totalSize = k;

        logger.log(Level.FINE, "k <= reorderSize: {0}", (k <= size));

        final int arraySize = (size + from) < k ? size
                : (k - from) > 0 ? (k - from) : 0;
        if (arraySize == 0) {
            return new InternalSearchHits(new InternalSearchHit[0], 0, 0);
        }

        InternalSearchHit[] newSearchHits = new InternalSearchHit[arraySize];
        k = 0;
        for (int i = from; i < arraySize + from; i++) {
            InternalSearchHit newId = tmpSearchHits[i];
            if (newId == null) {
                break;
            }
            newSearchHits[k++] = newId;
        }
        return new InternalSearchHits(newSearchHits, totalSize,
                hits.maxScore());
    }

    protected Set<String> getFilteredItems() {
        CypherResult result = getSqlResult();

        Set<String> filteredItems = new HashSet<>();

        for (ResultRow resultRow : result.getRows()) {
            filteredItems.add(getFilteredItem(resultRow));
        }

        return filteredItems;
    }

    protected CypherResult getSqlResult() {
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
        CypherResult result = new CypherResult();
    
        try {
            // Establish the connection.
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                con = DriverManager.getConnection(sqlConnectionString);
                stmt = con.createStatement();
                rs = stmt.executeQuery(sqlQuery);
                result = buildResult(rs);
        }
        
		// Handle any errors that may have occurred.
		catch (Exception e) {
            esLogger.debug("getSqlResult - Exception:" + e.getMessage());
		}

        finally
        {
   			if (rs != null) try { rs.close(); } catch(Exception e) {}
    		if (stmt != null) try { stmt.close(); } catch(Exception e) {}
    		if (con != null) try { con.close(); } catch(Exception e) {}
            return result;
        }
    }

    protected String getFilteredItem(ResultRow resultRow) {
        if (!resultRow.getValues().containsKey(getIdResultName())) {
            throw new RuntimeException("The sql query result must contain the " + getIdResultName() + " column name");
        }

        return getIdentifier(resultRow.get(getIdResultName()));
    }

    public String getIdResultName() {
        return null != idResultName ? idResultName : DEFAULT_ID_RESULT_NAME;
    }

    private static String getIdentifier(Object objectId) {
        if (objectId instanceof String) {
            return (String) objectId;
        }
        return String.valueOf(objectId);
    }
    
// Make the SQLResult into a Cypher Result
    private CypherResult buildResult(ResultSet response) throws SQLException {
        CypherResult result = new CypherResult();
        ResultSetMetaData rsmd = response.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while (response.next()) {
            ResultRow resultRow = new ResultRow();
            // The column count starts from 1
            for (int i = 1; i <= columnCount; i++ ) {
                String name = rsmd.getColumnName(i);
                String value = response.getString(i);
                esLogger.debug("buildResult - name:value =" + name +":" + value);
                resultRow.add(name,value);
            }
            result.addRow(resultRow);
        }
        return result;
    }

}