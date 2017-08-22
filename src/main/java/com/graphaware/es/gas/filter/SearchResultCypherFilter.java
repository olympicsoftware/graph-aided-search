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
package com.graphaware.es.gas.filter;

import com.graphaware.es.gas.annotation.SearchFilter;
import com.graphaware.es.gas.cypher.CypherEndPoint;
import com.graphaware.es.gas.cypher.CypherResult;
import com.graphaware.es.gas.cypher.CypherSettingsReader;
import com.graphaware.es.gas.cypher.ResultRow;
import com.graphaware.es.gas.domain.IndexInfo;
import com.graphaware.es.gas.util.NumberUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.graphaware.es.gas.domain.ClauseConstants.*;
import static com.graphaware.es.gas.wrap.GraphAidedSearchActionListenerWrapper.GAS_FILTER_CLAUSE;

@SearchFilter(name = "SearchResultCypherFilter")
public class SearchResultCypherFilter extends CypherSettingsReader implements SearchResultFilter {

    private static final Logger logger = Logger.getLogger(SearchResultCypherFilter.class.getName());

    private static final String DEFAULT_ID_RESULT_NAME = "id";
    private static final String ID_RESULT_NAME_KEY = "identifier";
    private static final int DEFAULT_RESULT_SIZE = 10;
    private static final int DEFAULT_FROM_VALUE = 0;
    protected static final String DEFAULT_PROTOCOL = "http";

    private int maxResultSize = -1;
    private CypherEndPoint cypherEndPoint;

    private int size;
    private int from;
    private String cypherQuery;
    private boolean shouldExclude = true;
    private String idResultName;

    public SearchResultCypherFilter(Settings settings, IndexInfo indexSettings) {
        super(settings, indexSettings);
    }

    @Override
    public void parseRequest(Map<String, Object> sourceAsMap) {
        size = NumberUtil.getInt(sourceAsMap.get(SIZE), DEFAULT_RESULT_SIZE);
        from = NumberUtil.getInt(sourceAsMap.get(FROM), DEFAULT_FROM_VALUE);

        HashMap extParams = (HashMap) sourceAsMap.get(GAS_FILTER_CLAUSE);
        if (extParams != null) {
            cypherQuery = (String) extParams.get(QUERY);
            maxResultSize = NumberUtil.getInt(extParams.get(MAX_RESULT_SIZE), getMaxResultWindow());
            shouldExclude = extParams.containsKey(EXCLUDE) && String.valueOf(extParams.get(EXCLUDE)).equalsIgnoreCase(TRUE);
            idResultName = extParams.containsKey(ID_RESULT_NAME_KEY) ? String.valueOf(extParams.get(ID_RESULT_NAME_KEY)) : null;
            String protocol = extParams.containsKey(PROTOCOL) ? String.valueOf(extParams.get(PROTOCOL)) : DEFAULT_PROTOCOL;
            cypherEndPoint = createCypherEndPoint(protocol, getSettings());
        }
        if (maxResultSize > 0) {
            sourceAsMap.put(SIZE, maxResultSize);
        }
        if (null == cypherQuery) {
            throw new RuntimeException("The Query Parameter is required in gas-filter");
        }
    }

    @Override
    public InternalSearchHits modify(final InternalSearchHits hits) {
        Set<String> remoteFilter = getFilteredItems();
        final InternalSearchHit[] searchHits = hits.internalHits();
        Map<String, InternalSearchHit> hitMap = new HashMap<>();
        for (InternalSearchHit hit : searchHits) {
            hitMap.put(hit.getId(), hit);
        }

        InternalSearchHit[] tmpSearchHits = new InternalSearchHit[hitMap.size()];
        int k = 0;
        float maxScore = -1;
        for (Map.Entry<String, InternalSearchHit> item : hitMap.entrySet()) {
            if ((shouldExclude && !remoteFilter.contains(item.getKey()))
                    || (!shouldExclude && remoteFilter.contains(item.getKey()))) {
                tmpSearchHits[k] = item.getValue();
                k++;
                float score = item.getValue().getScore();
                if (maxScore < score) {
                    maxScore = score;
                }
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
        CypherResult result = getCypherResult();
        Set<String> filteredItems = new HashSet<>();

        for (ResultRow resultRow : result.getRows()) {
            filteredItems.add(getFilteredItem(resultRow));
        }

        return filteredItems;
    }

    protected CypherResult getCypherResult() {
        return cypherEndPoint.executeCypher(cypherQuery, new HashMap<String, Object>());
    }

    protected String getFilteredItem(ResultRow resultRow) {
        if (!resultRow.getValues().containsKey(getIdResultName())) {
            throw new RuntimeException("The cypher query result must contain the " + getIdResultName() + " column name");
        }

        return getIdentifier(resultRow.get(getIdResultName()));
    }

    public int getSize() {
        return size;
    }

    public int getFrom() {
        return from;
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
    
    
}
