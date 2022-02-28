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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

// Redundant over TestIcebergOrcConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like materialized views may be supported by Iceberg only.
public class TestIcebergConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE iceberg.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        format("   location = '%s/iceberg_data/tpch/region'\n", tempDir) +
                        ")");
    }
}
