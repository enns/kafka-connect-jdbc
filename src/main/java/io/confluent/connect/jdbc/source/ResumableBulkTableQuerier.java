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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.confluent.connect.jdbc.source.bulk.Column;
import io.confluent.connect.jdbc.source.bulk.ColumnValuesOffset;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class ResumableBulkTableQuerier extends TableQuerier {
    private static final Logger log = LoggerFactory.getLogger(ResumableBulkTableQuerier.class);
    private final Map<String, String> partition;
    private final String topic;
    private final String resumeQuery;
    private final List<String> offsetColumnNames;
    private final TableId offsetColumnsTable;
    private final Map<String, Object> offsetMap;

    private ColumnValuesOffset offset;

    public ResumableBulkTableQuerier(
            DatabaseDialect dialect,
            String query,
            String resumeQuery,
            String topicPrefix,
            String suffix,
            String offsetColumnsTableName,
            String[] offsetColumnNames,
            Map<String, Object> offsetMap
    ) {
        super(dialect, QueryMode.QUERY, query, topicPrefix, suffix);
        assert resumeQuery != null && !resumeQuery.trim().isEmpty();
        this.resumeQuery = resumeQuery;

        assert offsetColumnsTableName != null && !offsetColumnsTableName.trim().isEmpty();
        this.offsetColumnsTable = dialect.parseTableIdentifier(offsetColumnsTableName);

        assert offsetColumnNames != null && offsetColumnNames.length > 0;
        this.offsetColumnNames = Arrays.asList(offsetColumnNames);

        partition = Collections.singletonMap(
                JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        this.offsetMap = offsetMap;
    }

    private String getQuery() {
        return offset.isNotEmpty() ? resumeQuery : query;
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        // initialize offset columns
        initializeOffset(db);

        // initialize offset if provided
        offset.fromMap(this.offsetMap);

        switch (mode) {
            case QUERY:
                recordQuery(getQuery());
                log.debug("{} prepared SQL query: {}", this, getQuery());
                stmt = dialect.createPreparedStatement(db, getQuery());
                break;
            default:
                throw new ConnectException("Unsupported or unknown mode: " + mode);
        }
    }

    private void initializeOffset(Connection db) throws SQLException {
        Collection<ColumnDefinition> tableColumns = dialect.describeColumns(
                db,
                offsetColumnsTable.catalogName(),
                offsetColumnsTable.schemaName(),
                offsetColumnsTable.tableName(),
                null).values();
        Map<String, ColumnDefinition> map = new HashMap<>(tableColumns.size());
        tableColumns.forEach(c->map.put(c.id().name(), c));

        List<Column> columns = this.offsetColumnNames.stream()
                .map(name -> {
                    String nameInResultset;
                    if(name.contains(":")) {
                        final int index = name.indexOf(':');
                        if(index == 0 || index == name.length()-1) {
                            throw new IllegalArgumentException("Invalid offset column format");
                        }
                        nameInResultset = name.substring(index+1);
                        name = name.substring(0,index);
                    } else {
                        nameInResultset = name;
                    }

                    ColumnDefinition cd = map.get(name);
                    if (cd == null) {
                        throw new ConnectException("Unknown column "
                                + name
                                + " in table "
                                + this.offsetColumnsTable.tableName()
                        );
                    }
                    return new Column(name, nameInResultset, cd.type(), -1);
                }).collect(Collectors.toList());
        this.offset = new ColumnValuesOffset(columns);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        if (offset.isNotEmpty()) {
            offset.setParameters(stmt);
        }

        return stmt.executeQuery();
    }


    @Override
    public SourceRecord extractRecord() throws SQLException {
        Struct record = new Struct(schemaMapping.schema());
        for (FieldSetter setter : schemaMapping.fieldSetters()) {
            try {
                setter.setField(record, resultSet);
            } catch (IOException e) {
                log.warn("Ignoring record because processing failed:", e);
            } catch (SQLException e) {
                log.warn("Ignoring record due to SQL error:", e);
            }
        }
        offset.extractFrom(resultSet);
        return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
    }

    @Override
    public String toString() {
        return "ResumeableBulkTableQuerier{" + "table='" + tableId + '\''
                + ", query='" + query + '\''
                + ", resumeQuery='" + resumeQuery + '\''
                + ", topicPrefix='" + topicPrefix + '\'' + '}';
    }

}
