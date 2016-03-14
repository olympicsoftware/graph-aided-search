/*
 * Copyright (c) 2015 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.graphaware.integration.es.plugin;

import com.graphaware.integration.es.plugin.annotation.GAGraphBooster;
import com.graphaware.integration.es.plugin.graphbooster.IGAResultBooster;
import com.graphaware.integration.es.plugin.query.GASIndexInfo;
import com.graphaware.integration.es.plugin.util.GASUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

/**
 *
 * @author ale
 */
@GAGraphBooster(name = "GraphAidedSearchTestBooster")
public class GraphAidedSearchTestBooster implements IGAResultBooster {

    private final ESLogger logger;
    private final String neo4jHost;
    private final int maxResultWindow;

    private int size;
    private int from;
    private String targetId;
    private int maxResultSize = -1;

    public GraphAidedSearchTestBooster(Settings settings, GASIndexInfo indexSettings) {
        this.neo4jHost = indexSettings.getNeo4jHost();
        this.maxResultWindow = indexSettings.getMaxResultWindow();
        this.logger = Loggers.getLogger(GraphAidedSearchPlugin.INDEX_LOGGER_NAME, settings);
    }

    public void parseRequest(Map<String, Object> sourceAsMap) {
        size = GASUtil.getInt(sourceAsMap.get("size"), 10);
        from = GASUtil.getInt(sourceAsMap.get("from"), 0);

        HashMap extParams = (HashMap) sourceAsMap.get("ga-booster");
        if (extParams != null) {
            targetId = (String) extParams.get("recoTarget");
            maxResultSize = GASUtil.getInt(extParams.get("maxResultSize"), maxResultWindow);
            //keyProperty = (String) (extParams.get("keyProperty") != null ? extParams.get("keyProperty") : keyProperty);
        }
        if (maxResultSize > 0) {
            sourceAsMap.put("size", maxResultSize);
        }
        sourceAsMap.put("from", 0);
    }

    public InternalSearchHits doReorder(final InternalSearchHits hits) {
        final InternalSearchHit[] searchHits = hits.internalHits();
        Map<String, InternalSearchHit> hitMap = new HashMap<>();
        for (InternalSearchHit hit : searchHits) {
            hitMap.put(hit.getId(), hit);
        }
        int totalHitsSize = hitMap.keySet().size();
        Map<String, Neo4JResult> remoteScore = externalDoReorder(hitMap.keySet());
        final int arraySize = (size + from) < searchHits.length ? size
                : (searchHits.length - from) > 0 ? (searchHits.length - from) : 0;
        if (arraySize == 0) {
            return new InternalSearchHits(new InternalSearchHit[0], 0, 0);
        }

        final int totalSize = arraySize + from;
        List<InternalSearchHit> newSearchHits = new ArrayList<>(totalSize);
        float maxScore = -1;
        for (Map.Entry<String, InternalSearchHit> item : hitMap.entrySet()) {
            Neo4JResult remoteResult = remoteScore.get(item.getKey());
            if (remoteResult != null) {
                float newScore = item.getValue().score() * remoteResult.getScore();
                if (maxScore < newScore) {
                    maxScore = newScore;
                }
                item.getValue().score(newScore);
            }
            int k = 0;
            while (newSearchHits.size() > 0
                    && k < newSearchHits.size()
                    && newSearchHits.get(k) != null
                    && newSearchHits.get(k).score() > item.getValue().score()
                    && k < totalSize) {
                k++;
            }
            if (k < totalSize) {
                newSearchHits.add(k, item.getValue());
            }
            if (newSearchHits.size() > totalSize) {
                newSearchHits.remove(totalSize);
            }
        }
        if (from > 0) {
            int k = 0;
            while (k < from) {
                newSearchHits.remove(0);
                k++;
            }
        }
        return new InternalSearchHits(newSearchHits.toArray(new InternalSearchHit[arraySize]), totalHitsSize,
                maxScore);
    }

    public int getSize() {
        return size;
    }

    public int getFrom() {
        return from;
    }

    public int getMaxResultSize() {
        return maxResultSize;
    }

    private Map<String, Neo4JResult> externalDoReorder(Set<String> keySet) {
        Map<String, Neo4JResult> result = new HashMap<>();
        for (String id : keySet) {
            result.put(id, new Neo4JResult(id, Integer.parseInt(id) * 1000));
        }
        return result;
    }

}