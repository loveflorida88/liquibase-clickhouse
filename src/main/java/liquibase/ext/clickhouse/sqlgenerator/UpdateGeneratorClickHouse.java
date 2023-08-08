/*-
 * #%L
 * Liquibase extension for ClickHouse
 * %%
 * Copyright (C) 2020 - 2022 Mediarithmics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package liquibase.ext.clickhouse.sqlgenerator;

import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UpdateGenerator;
import liquibase.statement.core.UpdateStatement;
import liquibase.structure.core.Column;

public class UpdateGeneratorClickhouse extends UpdateGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(UpdateStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(UpdateStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ")
            .append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()))
            .append(SqlGeneratorUtil.generateSqlOnClusterClause(properties))
            .append(" UPDATE");
        for (String column : statement.getNewColumnValues().keySet()) {
            sql.append(" ")
                .append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), column))
                .append(" = ")
                .append(convertToString(statement.getNewColumnValues().get(column), database))
                .append(",");
        }

        int lastComma = sql.lastIndexOf(",");
        if (lastComma >= 0) {
            sql.deleteCharAt(lastComma);
        }
        if (statement.getWhereClause() != null) {
            sql.append(" WHERE ").append(replacePredicatePlaceholders(database, statement.getWhereClause(), statement.getWhereColumnNames(), statement.getWhereParameters()));
        }

        return new Sql[] {
                new UnparsedSql(sql.toString(), getAffectedTable(statement))
        };
  }
}
