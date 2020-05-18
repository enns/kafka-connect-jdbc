/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source.bulk;

import io.confluent.connect.jdbc.util.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnValuesOffset {
    private static final Logger log = LoggerFactory.getLogger(ColumnValuesOffset.class);

    private final List<Column> columns;
    private List<ColumnValue> offset;

    public ColumnValuesOffset(List<Column> columns) {
        this.columns = columns;
    }

    public ColumnValuesOffset(List<Column> columns, List<ColumnValue> offset) {
        this.columns = columns;
        this.offset = offset;
    }

    public static ColumnValuesOffsetBuilder builder() {
        return new ColumnValuesOffsetBuilder();
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setOffset(List<ColumnValue> offset) {
        this.offset = offset;
    }

    public void fromMap(Map<String, Object> map) {
        List<ColumnValue> offset = new ArrayList<>(columns.size());
        if(map != null && !map.isEmpty()) {
            for( Column c : columns ) {
                Object value = map.get(c.getName());
                if( value == null ) {
                    offset.clear();
                    log.error("Uncomplete offset. Missung value for {} column.", c.getName());
                    break;
                }
                offset.add( new ColumnValue(c, c.fromPrimitive(value)) );
            }
        }
        this.offset = offset;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(offset.size());
        offset.forEach(o -> map.put(o.getColumn().getName(), o.getValueAsPrimitive()));
        return map;
    }

    public void setParameters(PreparedStatement stmt) throws SQLException {
        for (int i = 0; i < offset.size(); i++) {
            ColumnValue columnValue = offset.get(i);
            stmt.setObject(i+1, columnValue.getValue(), columnValue.getColumn().getType());
        }
    }

    public boolean isEmpty() {
        return offset.isEmpty();
    }

    public boolean isNotEmpty() {
        return !offset.isEmpty();
    }

    private boolean isResultsetPositionInitialized() {
        return columns.get(0).isResultsetIndexProvided();
    }

    public void extractFrom(ResultSet resultSet) throws SQLException {
        if(!isResultsetPositionInitialized()) {
            initilizeResultsetPositions(resultSet);
        }

        this.offset = columns.stream().map(c -> {
            try {
                return new ColumnValue(c,
                        resultSet.getObject(c.getResultsetIndex()));
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Failed to extract " + c.getNameInResultset()
                                + " column value from result set", e);
            }
        }).collect(Collectors.toList());
    }

    private void initilizeResultsetPositions(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int numberOfColumns = metaData.getColumnCount();
        final Map<String, Integer> resultsetIndexMap = new HashMap<>(numberOfColumns);
        for (int i = 1; i <= numberOfColumns; i++) {
            String columnName = metaData.getColumnName(i);
            resultsetIndexMap.put(columnName, i);
        }

        this.columns.forEach(c-> {
            Integer resultsetIndex = resultsetIndexMap.get(c.getNameInResultset());
            if( resultsetIndex == null ) {
                throw new ConfigException("The field '"
                        + c.getNameInResultset()
                        + "' does not exist in result set. The result set consists of the following fields: "
                        + StringUtils.join(resultsetIndexMap.keySet(),",")
                );
            }
            c.setResultsetIndex(resultsetIndex);
        });
    }

    public static class ColumnValuesOffsetBuilder {
        private ColumnValuesOffset offset;

        private ColumnValuesOffsetBuilder() {
            this.offset = new ColumnValuesOffset(new ArrayList<>(), new ArrayList<>());
        }

        public ColumnValuesOffsetBuilder add(String columnName, String nameInResultset, int type, Object value) {
            Column column = new Column(columnName, nameInResultset, type, -1);
            offset.columns.add(column);
            offset.offset.add(new ColumnValue(column, value));

            return this;
        }

        public ColumnValuesOffset build() {
            return offset;
        }
    }
}
