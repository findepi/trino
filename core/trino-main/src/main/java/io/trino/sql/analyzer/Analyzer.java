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
package io.trino.sql.analyzer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.security.AccessControl;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.split.PageSourceManager;
import io.trino.split.PageSourceProvider;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final AnalyzerFactory analyzerFactory;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final Session session;
    private final List<Expression> parameters;
    private final Map<NodeRef<Parameter>, Expression> parameterLookup;
    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;
    private final Tracer tracer;
    private final StatementRewrite statementRewrite;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;

    Analyzer(
            Session session,
            AnalyzerFactory analyzerFactory,
            StatementAnalyzerFactory statementAnalyzerFactory,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            Tracer tracer,
            StatementRewrite statementRewrite,
            SplitManager splitManager,
            PageSourceManager pageSourceManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.parameters = ImmutableList.copyOf(parameters);
        this.parameterLookup = ImmutableMap.copyOf(parameterLookup);
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.statementRewrite = requireNonNull(statementRewrite, "statementRewrite is null");
        this.splitManager = splitManager;
        this.pageSourceManager = pageSourceManager;
    }

    public Analysis analyze(Statement statement, String originalSql)
    {
        Span span = tracer.spanBuilder("analyzer")
                .setParent(Context.current().with(session.getQuerySpan()))
                .startSpan();
        try (var _ = scopedSpan(span)) {
            return analyze(statement, OTHERS, originalSql);
        }
    }

    public Analysis analyze(Statement statement, QueryType queryType, String originalSql)
    {
        Statement rewrittenStatement = statementRewrite.rewrite(analyzerFactory, session, statement, parameters, parameterLookup, warningCollector, planOptimizersStatsCollector);
        Analysis analysis = new Analysis(rewrittenStatement, parameterLookup, queryType);
        StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

        try (var _ = scopedSpan(tracer, "analyze")) {
            analyzer.analyze(rewrittenStatement);
        }

        try (var _ = scopedSpan(tracer, "access-control")) {
            // check column access permissions for each table
            analysis.getTableColumnReferences().forEach((accessControlInfo, tableColumnReferences) ->
                    tableColumnReferences.forEach((tableName, columns) ->
                            accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                    accessControlInfo.getSecurityContext(session.getRequiredTransactionId(), session.getQueryId(), session.getStart()),
                                    tableName,
                                    columns)));
        }

        try {
            dumpQueryState(originalSql, analysis);
        }
        catch (Exception e) {
            // e.g. incorrectly wired MockConnectorFactory
            log.error(e, "Unhandled exception when dumping query state");
            String stackTraceAsString = Throwables.getStackTraceAsString(e);
            System.out.println();
        }

        return analysis;
    }

    private static final Logger log = Logger.get(Analyzer.class);

    private void dumpQueryState(String originalSql, Analysis analysis)
    {
        if (originalSql != null && analysis.getUpdateType() == null) {
            // not an update
            List<TableState> tableStates = new ArrayList<>();
            for (Analysis.TableEntry tableEntry : analysis.tables.values()) {
                QualifiedObjectName tableName = tableEntry.getName();
                Optional<TableHandle> tableHandle = tableEntry.getHandle();
                if (tableHandle.isEmpty()) {
                    // probably when MV without a storage table yet; Skip
                    return;
                }
                System.out.println("Dumping data from %s".formatted(tableName));
                Optional<TableState> dump = dumpTableData(tableEntry.getName(), tableHandle.get());
                if (dump.isEmpty()) {
                    // e.g. too big
                    System.out.println("E.g. data too big or something, not capturing query state");
                    return;
                }
                tableStates.add(dump.get());
            }

            String tableStatesJson = TABLE_STATES_CODEC.toJson(tableStates);
            @SuppressWarnings("deprecation")
            HashFunction sha1 = Hashing.sha1();
            String stateId = sha1.hashString(tableStatesJson, UTF_8).toString();

            Map<String, String> queryState = ImmutableMap.of(
                    "state", stateId,
                    "session_catalog", session.getCatalog().orElse(""),
                    "session_schema", session.getSchema().orElse(""),
                    "session_timezone", session.getTimeZoneKey().toString(),
                    "query", originalSql);
            String queryStateJson = MAP_CODEC.toJson(queryState);
            String queryDumpId = sha1.hashString(queryStateJson, UTF_8).toString();

            System.out.println("query state id is: %s (%.200s)".formatted(stateId, tableStatesJson.replace("\n", "\\n")));
            System.out.println("query is %.200s".formatted(originalSql));

            try {
                //  dump data to state file (if not exists)
                Path stateDir = Paths.get("/tmp/trino-test-dump/state");
                Files.createDirectories(stateDir);
                try {
                    Files.writeString(stateDir.resolve(stateId + ".state.json"), tableStatesJson, UTF_8, StandardOpenOption.CREATE_NEW);
                }
                catch (java.nio.file.FileAlreadyExistsException ignore) {
                }

                //  dump query + state id
                Path queryDir = Paths.get("/tmp/trino-test-dump/query");
                Files.createDirectories(queryDir);
                try {
                    Files.writeString(queryDir.resolve(queryDumpId + ".query.json"), queryStateJson, UTF_8, StandardOpenOption.CREATE_NEW);
                }
                catch (java.nio.file.FileAlreadyExistsException ignore) {
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static JsonCodec<Map<String, String>> MAP_CODEC = io.airlift.json.JsonCodec.mapJsonCodec(String.class, String.class);
    static JsonCodec<List<TableState>> TABLE_STATES_CODEC = io.airlift.json.JsonCodec.listJsonCodec(TableState.class);

    public record TableState(QualifiedObjectName tableName, List<ColumnInfo> columns, List<List<String>> data) {}

    public record ColumnInfo(String name, String type) {}

    Optional<TableState> dumpTableData(QualifiedObjectName tableName, TableHandle tableHandle)
    {
        if (splitManager == null || pageSourceManager == null) {
            // not wired in
            return Optional.empty();
        }

        DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
        Constraint constraint = Constraint.alwaysTrue();

        Metadata metadata = statementAnalyzerFactory.plannerContext.getMetadata();
        TableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
        // Map<String, ColumnHandle> columnHandlesByName = metadata.getColumnHandles(session, tableHandle);
        List<String> columnNames = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        List<ColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnInfo> columnInfos = new ArrayList<>();
        metadata.getColumnHandles(session, tableHandle).forEach((name, columnHandle) -> {
            if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
                return;
            }
            columnNames.add(name);
            Type type = tableSchema.column(name).getType();
            types.add(type);
            columnHandles.add(columnHandle);
            columnInfos.add(new ColumnInfo(name, type.getDisplayName()));
        });

        String columnNamesString = columnNames.stream()
                .map(name -> '"' + name.replace("\"", "\"\"") + '"')
                .collect(Collectors.joining(", "));

        List<Function<Object, String>> literalizers = types.stream()
                .map(this::literalizer)
                .toList();
        int columns = columnNames.size();

        Span span = tracer.spanBuilder("dumpTableData").startSpan();
        ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
        long dumpDataSize = 0;
        try (var _ = scopedSpan(span)) {
            try (SplitSource splitSource = splitManager.getSplits(session, span, tableHandle, dynamicFilter, constraint)) {
                PageSourceProvider pageSourceProvider = pageSourceManager.createPageSourceProvider(tableHandle.catalogHandle());
                while (!splitSource.isFinished()) {
                    SplitSource.SplitBatch splitBatch = splitSource.getNextBatch(10).get(30, TimeUnit.SECONDS);
                    for (Split split : splitBatch.getSplits()) {
                        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, split, tableHandle, columnHandles, dynamicFilter)) {
                            while (!pageSource.isFinished()) {
                                Page page = pageSource.getNextPage();
                                if (page != null) {
                                    for (int position = 0; position < page.getPositionCount(); position++) {
                                        ImmutableList.Builder<String> row = ImmutableList.builderWithExpectedSize(columns);
                                        for (int column = 0; column < columns; column++) {
                                            Type type = types.get(column);
                                            Object value = readNativeValue(type, page.getBlock(column), position);
                                            String valueString = (value == null)
                                                    ? "NULL"
                                                    : literalizers.get(column).apply(value);
                                            dumpDataSize += valueString.length();
                                            if (dumpDataSize > 1_000_000) {
                                                return Optional.empty();
                                            }
                                            row.add(valueString);
                                        }
                                        rows.add(row.build());
                                    }
                                }
                            }
                        }
                    }
                    if (splitBatch.isLastBatch()) {
                        break;
                    }
                }
            }
        }
        catch (TimeoutException | IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        List<List<String>> rowsList = rows.build();
        return Optional.of(
                new TableState(
                        tableName,
                        columnInfos,
                        rowsList
                                .stream()
                                // normalize
                                .sorted(Ordering.natural().lexicographical())
                                .toList())
        );
    }

    Function<Object, String> literalizer(Type type)
    {
        String typeName = type.getDisplayName();

        // Specialized short formatters to reduce the output size
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return value -> Long.toString(((Number) value).longValue());
        }
        if (type instanceof VarcharType) {
            return value -> "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }

        // Better not mess with JSON numbers if not necessary
        if (type == REAL || type == DOUBLE || type instanceof DecimalType) {
            ResolvedFunction castToVarchar = statementAnalyzerFactory.plannerContext.getMetadata().getCoercion(type, VARCHAR);
            IrExpressionEvaluator evaluator = new IrExpressionEvaluator(statementAnalyzerFactory.plannerContext);
            return value -> {
                Slice varchar = (Slice) evaluator.evaluate(
                        new Call(castToVarchar, List.of(new Constant(type, value))),
                        session,
                        Map.of());
                return "CAST('%s' AS %s)".formatted(varchar.toStringUtf8().replace("'", "''"), typeName);
            };
        }

        ResolvedFunction castToJson = statementAnalyzerFactory.plannerContext.getMetadata().getCoercion(type, JSON);
        IrExpressionEvaluator evaluator = new IrExpressionEvaluator(statementAnalyzerFactory.plannerContext);

        return value -> {
            Slice json = (Slice) evaluator.evaluate(
                    new Call(castToJson, List.of(new Constant(type, value))),
                    session,
                    Map.of());
            return "CAST(JSON '%s' AS %s)".formatted(json.toStringUtf8().replace("'", "''"), typeName);
        };
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(Session session, FunctionResolver functionResolver, AccessControl accessControl, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), session, functionResolver, accessControl);

        List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate), session, functionResolver, accessControl);

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw semanticException(EXPRESSION_NOT_SCALAR, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }
}
