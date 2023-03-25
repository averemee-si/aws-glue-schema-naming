/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.aws.glue.schema.registry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JsonSchemaNamingStrategy implements AWSSchemaNamingStrategy {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaNamingStrategy.class);

	/**
	 * Returns the schemaName.
	 * If data passed are in com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
	 * format 'connect.name' of schema is returned
	 *
	 * @param transportName topic Name or stream name etc.
	 * @param data
	 * @return schema name.
	 */
	@Override
	public String getSchemaName(String transportName, Object data) {
		if (data instanceof JsonDataWithSchema) {
			return getConnectName((JsonDataWithSchema) data, transportName);
		} else {
			return getSchemaName(transportName);
		}
	}

	/**
	 * Returns the schemaName.
	 * If data passed are in com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
	 * format 'connect.name' of schema is returned
	 *
	 * @param transportName topic Name or stream name etc.
	 * @param data
	 * @param isKey
	 * @return schema name.
	 */
	@Override
	public String getSchemaName(String transportName, Object data, boolean isKey) {
		if (data instanceof JsonDataWithSchema) {
			return getConnectName((JsonDataWithSchema) data, transportName);
		} else {
			return getSchemaName(transportName);
		}
	}

	private String getConnectName(final JsonDataWithSchema record, final String transportName) {
		final String schemaName = 
			StringUtils.substringBetween(
				StringUtils.substringBefore(
					StringUtils.substringAfter(
						StringUtils.substringAfter(record.getSchema(), "\"connect.name\""),
					":"),
				","),
			"\"");
		
		LOGGER.debug("connect.name='{}' will be used as SchemaName for data in topic {}",
				schemaName, transportName);
		return schemaName;
	}

	/**
	 * Returns the schemaName.
	 *
	 * @param transportName topic Name or stream name etc.
	 * @return schema name.
	 */
	@Override
	public String getSchemaName(String transportName) {
		return transportName;
	}

}
