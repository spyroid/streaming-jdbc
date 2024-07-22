package symetricum.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@RequiredArgsConstructor
public class StreamingJdbcCall<T> {
    private final DataSource dataSource;
    private final String procedureName;
    private final Supplier<RowMapper<T>> rowMapper;

    public Stream<T> execute(Object... params) {
        var runSP = "{ call " + procedureName + "(" + IntStream.range(0, params.length + 1)
                .boxed().map(i -> "?").collect(Collectors.joining(",")) + ") }";

        var connection = getConnection();
        CallableStatement callableStatement = null;
        ResultSet resultSet = null;

        try {
            callableStatement = connection.prepareCall(runSP);
            for (int i = 0; i < params.length; i++) callableStatement.setObject(i + 1, params[i]);
            callableStatement.registerOutParameter(params.length + 1, Types.REF_CURSOR);
            callableStatement.execute();
            resultSet = callableStatement.getObject(params.length + 1, ResultSet.class);
            CallableStatement finalCallableStatement = callableStatement;
            ResultSet finalResultSet = resultSet;
            return JdbcStream.stream(resultSet, rowMapper.get()).onClose(() -> cleanup(finalCallableStatement, finalResultSet, connection));
        } catch (Exception e) {
            cleanup(callableStatement, resultSet, connection);
        }
        return Stream.of();
    }

    protected void cleanup(CallableStatement callableStatement, ResultSet resultSet, Connection connection) {
        JdbcUtils.closeStatement(callableStatement);
        JdbcUtils.closeResultSet(resultSet);
        DataSourceUtils.releaseConnection(connection, dataSource);
    }

    protected Connection getConnection() {
        return Objects.requireNonNull(DataSourceUtils.getConnection(dataSource));
    }
}

@Slf4j
class JdbcStream {
    public static Stream<ResultSet> stream(ResultSet resultSet) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(0, 0) {

            @SneakyThrows
            @Override
            public boolean tryAdvance(Consumer<? super ResultSet> action) {
                if (resultSet.next()) {
                    action.accept(resultSet);
                    return true;
                }
                return false;
            }
        }, false);
    }

    public static <TEntity> Stream<TEntity> stream(ResultSet resultSet, RowMapper<TEntity> rowMapper) {
        var i = new AtomicInteger();
        return stream(resultSet).map(rs -> {
            try {
                return rowMapper.mapRow(rs, i.getAndIncrement());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}