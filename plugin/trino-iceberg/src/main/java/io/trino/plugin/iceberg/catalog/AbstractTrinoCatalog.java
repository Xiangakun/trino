/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog;

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    protected final IcebergTableOperationsProvider tableOperationsProvider;
    private final boolean useUniqueTableLocation;

    protected AbstractTrinoCatalog(
            IcebergTableOperationsProvider tableOperationsProvider,
            boolean useUniqueTableLocation)
    {
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
    }

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }
}
