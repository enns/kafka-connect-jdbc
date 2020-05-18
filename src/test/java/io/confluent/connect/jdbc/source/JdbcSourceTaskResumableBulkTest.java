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

import io.confluent.connect.jdbc.source.bulk.ColumnValuesOffset;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

// Tests of polling that return data updates, i.e. verifies the different behaviors for getting
// incremental data updates from the database
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskResumableBulkTest extends JdbcSourceTaskTestBase {
    private static final Map<String, String> QUERY_SOURCE_PARTITION
            = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
            JdbcSourceConnectorConstants.QUERY_NAME_VALUE);

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneOffset.UTC);

    @After
    public void tearDown() throws Exception {
        task.stop();
        super.tearDown();
    }

    @Test
    public void testResumableBulkLoadNoStartOffset() throws Exception {
        EmbeddedDerby.ColumnName column = new EmbeddedDerby.ColumnName("id");
        db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);

        expectInitialize(
                Arrays.asList(QUERY_SOURCE_PARTITION),
                Collections.singletonMap(QUERY_SOURCE_PARTITION, new HashMap<>())
        );

        PowerMock.replayAll();

        initializeTask();

        // Bulk periodic load is currently the default
        Map<String, String> properties = singleTableConfig();
        properties.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK_RESUMABLE);
        properties.put(JdbcSourceConnectorConfig.QUERY_CONFIG,
                "select \"id\" as \"uid\" from \"" + SINGLE_TABLE_NAME + "\" order by \"uid\" asc");
        properties.put(JdbcSourceConnectorConfig.BULK_RESUME_QUERY_CONFIG,
                "select \"id\" as \"uid\" from \"" + SINGLE_TABLE_NAME + "\" where \"id\">=? order by \"uid\" asc");

        properties.put(JdbcSourceConnectorConfig.BULK_OFFSET_COLUMNS_TABLE_CONFIG, SINGLE_TABLE_NAME);

        properties.put(JdbcSourceConnectorConfig.BULK_OFFSET_COLUMNS_CONFIG, "id:uid");

        task.start(properties);

        List<SourceRecord> records = task.poll();
        Map<Integer, Integer> twoRecords = new HashMap<>();
        twoRecords.put(1, 1);
        twoRecords.put(2, 1);
        assertEquals(twoRecords, countIntValues(records, "uid"));
        assertRecordsTopic(records, TOPIC_PREFIX);

        db.insert(SINGLE_TABLE_NAME, "id", 3);
        records = task.poll();
        Map<Integer, Integer> threeRecords = new HashMap<>();
        threeRecords.put(3, 1);
        threeRecords.put(2, 1);
        threeRecords.put(1, 1);
        assertEquals(threeRecords, countIntValues(records, "uid"));
        assertRecordsTopic(records, TOPIC_PREFIX);

        PowerMock.verifyAll();
    }

    @Test
    public void testResumableBulkLoadWithStartOffset() throws Exception {
        EmbeddedDerby.ColumnName column = new EmbeddedDerby.ColumnName("id");
        db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);

        ColumnValuesOffset offset = ColumnValuesOffset.builder()
                .add("id", "id", Types.INTEGER, 2)
                .build();
        expectInitialize(
                Arrays.asList(QUERY_SOURCE_PARTITION),
                Collections.singletonMap(QUERY_SOURCE_PARTITION, offset.toMap())
        );

        PowerMock.replayAll();

        initializeTask();

        // Bulk periodic load is currently the default
        Map<String, String> properties = singleTableConfig();
        properties.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK_RESUMABLE);
        properties.put(JdbcSourceConnectorConfig.QUERY_CONFIG,
                "select * from \"" + SINGLE_TABLE_NAME + "\" order by \"id\" asc");
        properties.put(JdbcSourceConnectorConfig.BULK_RESUME_QUERY_CONFIG,
                "select * from \"" + SINGLE_TABLE_NAME + "\" where \"id\">=? order by \"id\" asc");

        properties.put(JdbcSourceConnectorConfig.BULK_OFFSET_COLUMNS_TABLE_CONFIG, SINGLE_TABLE_NAME);

        properties.put(JdbcSourceConnectorConfig.BULK_OFFSET_COLUMNS_CONFIG, "id");

        task.start(properties);

        List<SourceRecord> records = task.poll();
        assertEquals(Collections.singletonMap(2, 1), countIntValues(records, "id"));
        assertRecordsTopic(records, TOPIC_PREFIX);

        db.insert(SINGLE_TABLE_NAME, "id", 3);
        records = task.poll();
        Map<Integer, Integer> twoRecords = new HashMap<>();
        twoRecords.put(3, 1);
        twoRecords.put(2, 1);
        assertEquals(twoRecords, countIntValues(records, "id"));
        assertRecordsTopic(records, TOPIC_PREFIX);

        db.delete(SINGLE_TABLE_NAME, new EmbeddedDerby.EqualsCondition(column, 2));
        records = task.poll();
        assertEquals(Collections.singletonMap(3, 1), countIntValues(records, "id"));
        assertRecordsTopic(records, TOPIC_PREFIX);

        PowerMock.verifyAll();
    }

    private void assertRecordsTopic(List<SourceRecord> records, String topic) {
        for (SourceRecord record : records) {
            assertEquals(topic, record.topic());
        }
    }


    private enum Field {
        KEY,
        VALUE,
        TIMESTAMP_VALUE,
        INCREMENTING_OFFSET,
        TIMESTAMP_OFFSET
    }

    private Map<Integer, Integer> countIntValues(List<SourceRecord> records, String fieldName) {
        return countInts(records, JdbcSourceTaskResumableBulkTest.Field.VALUE, fieldName);
    }

    @SuppressWarnings("unchecked")
    private <T> Map<T, Integer> countInts(List<SourceRecord> records, JdbcSourceTaskResumableBulkTest.Field field, String fieldName) {
        Map<T, Integer> result = new HashMap<>();
        for (SourceRecord record : records) {
            T extracted;
            switch (field) {
                case KEY:
                    extracted = (T) record.key();
                    break;
                case VALUE:
                    extracted = (T) ((Struct) record.value()).get(fieldName);
                    break;
                case TIMESTAMP_VALUE: {
                    java.util.Date rawTimestamp = (java.util.Date) ((Struct) record.value()).get(fieldName);
                    extracted = (T) (Long) rawTimestamp.getTime();
                    break;
                }
                case INCREMENTING_OFFSET: {
                    TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
                    extracted = (T) (Long) offset.getIncrementingOffset();
                    break;
                }
                case TIMESTAMP_OFFSET: {
                    TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
                    Timestamp rawTimestamp = offset.getTimestampOffset();
                    extracted = (T) (Long) rawTimestamp.getTime();
                    break;
                }
                default:
                    throw new RuntimeException("Invalid field");
            }
            Integer count = result.get(extracted);
            count = (count != null ? count : 0) + 1;
            result.put(extracted, count);
        }
        return result;
    }

}
