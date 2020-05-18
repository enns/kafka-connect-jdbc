package io.confluent.connect.jdbc.source.bulk;

import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ColumnValuesOffsetTest {

    private ColumnValuesOffset offset;

    @Before
    public void setup() {
        List<Column> list = new ArrayList<>();
        list.add(new Column("intcol", "intcol", Types.INTEGER, 1));
        list.add(new Column("smallintcol", "smallintcol", Types.SMALLINT, 2));
        list.add(new Column("tscol", "tscol", Types.TIMESTAMP, 3));
        list.add(new Column("datecol", "datecol", Types.DATE, 4));
        this.offset = new ColumnValuesOffset(list);
    }

    @Test
    public void fromToMap() {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("intcol", 200);
        offsetMap.put("smallintcol", 22);
        long time = System.currentTimeMillis();
        offsetMap.put("tscol", time);
        offsetMap.put("datecol", time);
        offset.fromMap(offsetMap);

        Map<String, Object> newMap = offset.toMap();
        assertEquals(offsetMap, newMap);
    }
}