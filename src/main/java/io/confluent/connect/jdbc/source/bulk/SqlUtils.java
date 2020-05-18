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

import java.sql.Types;

public class SqlUtils {

    public static Object parseValue(String value, int type) {
        switch (type) {
            case Types.SMALLINT:
            case Types.INTEGER:
                return new Integer(value);

            case Types.VARCHAR:
            case Types.CHAR:
                return value;
            default:
                throw new UnsupportedOperationException("Type " + type + " is not supported.");
        }
    }

    public static String valueToString(Object value, int type) {
        switch (type) {
            case Types.VARCHAR:
            case Types.CHAR:
                return (String) value;
            default:
                return value.toString();
        }
    }
}
