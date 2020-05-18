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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

public class Column {
    private final String name;
    private final String nameInResultset;
    private final int type;
    private int resultsetIndex;

    public Column(String name, String nameInResultset, int type, int resultsetIndex) {
        this.name = name;
        this.nameInResultset = nameInResultset;
        this.type = type;
        this.resultsetIndex = resultsetIndex;
    }

    public String getName() {
        return name;
    }

    public String getNameInResultset() {
        return nameInResultset;
    }

    public int getType() {
        return type;
    }

    public int getResultsetIndex() {
        return resultsetIndex;
    }

    public boolean isResultsetIndexProvided() {
        return resultsetIndex > 0;
    }

    public void setResultsetIndex(int resultsetIndex) {
        this.resultsetIndex = resultsetIndex;
    }

    /**
     * Converts a primitive value to SQL-specific one
     * @param value primitive
     * @return SQL specific value
     */
    public Object fromPrimitive(Object value) {
        if( value == null ) {
            return null;
        }
        switch( type ) {
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return new Timestamp( (Long)value );
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return new Time( (Long)value );
            case Types.DATE:
                return new Date( (Long)value );
            default:
                return value;
        }
    }

    /**
     * Converts SQL value to promitive one
     * @param value SQL specific value
     * @return primitive value
     */
    public Object toPrimitive(Object value) {
        if( value == null ) {
            return null;
        }
        switch( type ) {
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                Timestamp timestamp = (Timestamp) value;
                return timestamp.getTime();
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                Time time = (Time) value;
                return time.getTime();
            case Types.DATE:
                Date date = (Date) value;
                return date.getTime();
            default:
                return value;
        }
    }
}
