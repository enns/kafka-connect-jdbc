package io.confluent.connect.jdbc.source.bulk;

import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static org.junit.Assert.*;

public class ColumnTest {

    @Test
    public void toPrimitiveString() {
        Column column = new Column("testcolumn", "testcolumn", Types.VARCHAR, 1);
        Object primitive = column.toPrimitive("testvalue");
        assertEquals("testvalue", primitive);
    }

    @Test
    public void fromPrimitiveString() {
        Column column = new Column("testcolumn", "testcolumn", Types.VARCHAR, 1);
        Object primitive = column.fromPrimitive("testvalue");
        assertEquals("testvalue", primitive);
    }

    @Test
    public void toPrimitiveTimestamp() {
        Column column = new Column("testcolumn", "testcolumn", Types.TIMESTAMP, 1);
        long time = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(time);
        Object primitive = column.toPrimitive(timestamp);
        assertEquals(time, ((Long)primitive).longValue());
    }

    @Test
    public void fromPrimitiveTimestamp() {
        Column column = new Column("testcolumn", "testcolumn", Types.TIMESTAMP, 1);
        long timestamp = System.currentTimeMillis();
        Object primitive = column.fromPrimitive(timestamp);
        assertEquals(timestamp, ((Timestamp)primitive).getTime());
    }

    @Test
    public void fromPrimitiveDate() {
        Column column = new Column("testcolumn", "testcolumn", Types.DATE, 1);
        Date timestamp = new Date(System.currentTimeMillis());
        Object obj = column.fromPrimitive(timestamp.getTime());
        assertTrue(obj instanceof Date);
        assertEquals(timestamp.getTime(), ((Date)obj).getTime());
    }

    @Test
    public void toPrimitiveDate() {
        Column column = new Column("testcolumn", "testcolumn", Types.DATE, 1);
        Date date = new Date(System.currentTimeMillis());
        Object obj = column.toPrimitive(date);
        assertTrue(obj instanceof Long);
        assertEquals(date.getTime(), obj);
    }
}