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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class AvroSchemaNamingStrategy implements AWSSchemaNamingStrategy {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaNamingStrategy.class);

	/**
	 * Returns the schemaName.
	 * If data passed are in org.apache.avro.generic.GenericData$Record format
	 * data.getSchema().getFullName() is returned ('connect.name' of schema)
	 *
	 * @param transportName topic Name or stream name etc.
	 * @param data
	 * @return schema name.
	 */
	@Override
	public String getSchemaName(String transportName, Object data) {
		if (data instanceof GenericData.Record) {
			return getConnectName((GenericData.Record) data, transportName);
		} else {
			return getSchemaName(transportName);
		}
	}

	/**
	 * Returns the schemaName.
	 * If data passed are in org.apache.avro.generic.GenericData$Record format
	 * data.getSchema().getFullName() is returned ('connect.name' of schema)
	 *
	 * @param transportName topic Name or stream name etc.
	 * @param data
	 * @param isKey
	 * @return schema name.
	 */
	@Override
	public String getSchemaName(String transportName, Object data, boolean isKey) {
		if (data instanceof GenericData.Record) {
			return getConnectName((GenericData.Record) data, transportName);
		} else {
			return getSchemaName(transportName);
		}
	}

	private String getConnectName(final GenericData.Record record, final String transportName) {
		final Schema schema = record.getSchema();
		LOGGER.debug("connect.name='{}' will be used as SchemaName for data in topic {}",
				schema.getFullName(), transportName);
		return schema.getFullName();
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
