/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.SchemaChangeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;

import java.util.ArrayList;
import java.util.List;

/** Schema update. */
public class SchemaUpdate {

    /** Apply schema changes to the given table info and return the updated schema. */
    public static Schema applySchemaChanges(Schema initialSchema, List<TableChange> changes) {
        SchemaUpdate schemaUpdate = new SchemaUpdate(initialSchema);
        for (TableChange change : changes) {
            schemaUpdate = schemaUpdate.applySchemaChange(change);
        }
        return schemaUpdate.getSchema();
    }

    // Now we only maintain the Builder
    private Schema.Builder builder;

    public SchemaUpdate(Schema initialSchema) {
        // Initialize builder from the current table schema
        this.builder = Schema.newBuilder().fromSchema(initialSchema);
    }

    public Schema getSchema() {
        // Validation and building are now delegated
        return builder.build();
    }

    private SchemaUpdate applySchemaChange(TableChange columnChange) {
        if (columnChange instanceof TableChange.AddColumn) {
            return addColumn((TableChange.AddColumn) columnChange);
        } else if (columnChange instanceof TableChange.ModifyColumn) {
            return modifiedColumn((TableChange.ModifyColumn) columnChange);
        } else if (columnChange instanceof TableChange.RenameColumn) {
            return renameColumn((TableChange.RenameColumn) columnChange);
        } else if (columnChange instanceof TableChange.DropColumn) {
            return dropColumn((TableChange.DropColumn) columnChange);
        }
        throw new IllegalArgumentException(
                "Unknown column change type " + columnChange.getClass().getName());
    }

    private SchemaUpdate addColumn(TableChange.AddColumn addColumn) {
        if (builder.getColumn(addColumn.getName()).isPresent()) {
            throw new InvalidAlterTableException(
                    "Column " + addColumn.getName() + " already exists.");
        }
        if (!addColumn.getDataType().isNullable()) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " must be nullable.");
        }

        if (addColumn.getPosition() == TableChange.ColumnPosition.last()) {
            builder.column(addColumn.getName(), addColumn.getDataType());
            String comment = addColumn.getComment();
            if (comment != null) {
                builder.withComment(comment);
            }
        } else if (addColumn.getPosition() instanceof TableChange.After) {
            String afterColName = ((TableChange.After) addColumn.getPosition()).columnName();
            // Rebuild the schema with the new column spliced in after afterColName so that callers
            // can insert before reserved trailing columns (e.g. event_time / headers on typed
            // Kafka tables) rather than always appending at the absolute end.
            Schema current = builder.build();
            List<Schema.Column> cols = new ArrayList<>(current.getColumns());
            int insertIdx = -1;
            for (int i = 0; i < cols.size(); i++) {
                if (cols.get(i).getName().equals(afterColName)) {
                    insertIdx = i + 1;
                    break;
                }
            }
            if (insertIdx < 0) {
                throw new InvalidAlterTableException(
                        "Column '" + afterColName + "' not found; cannot add after it.");
            }
            // Allocate a fresh column ID by running through a temporary single-column builder so
            // that ReassignFieldId fires correctly for nested types.
            Schema.Builder temp = Schema.newBuilder().highestFieldId(current.getHighestFieldId());
            temp.column(addColumn.getName(), addColumn.getDataType());
            if (addColumn.getComment() != null) {
                temp.withComment(addColumn.getComment());
            }
            Schema.Column newCol = temp.build().getColumns().get(0);
            cols.add(insertIdx, newCol);
            // Rebuild the main builder from the complete, reordered column list.
            Schema.Builder fresh = Schema.newBuilder().fromColumns(cols);
            current.getPrimaryKey()
                    .ifPresent(
                            pk ->
                                    fresh.primaryKeyNamed(
                                            pk.getConstraintName(), pk.getColumnNames()));
            for (String autoCol : current.getAutoIncrementColumnNames()) {
                fresh.enableAutoIncrement(autoCol);
            }
            this.builder = fresh;
        } else {
            throw new IllegalArgumentException(
                    "Only LAST and AFTER(<column>) positions are supported for AddColumn.");
        }

        return this;
    }

    private SchemaUpdate dropColumn(TableChange.DropColumn dropColumn) {
        throw new SchemaChangeException("Not support drop column now.");
    }

    private SchemaUpdate modifiedColumn(TableChange.ModifyColumn modifyColumn) {
        throw new SchemaChangeException("Not support modify column now.");
    }

    private SchemaUpdate renameColumn(TableChange.RenameColumn renameColumn) {
        throw new SchemaChangeException("Not support rename column now.");
    }
}
