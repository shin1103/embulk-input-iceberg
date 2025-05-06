package io.github.shin1103.embulk.input.iceberg;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.util.List;
import java.util.Optional;

public interface IcebergFilterOption extends Task
{
    @Config("type")
    @ConfigDefault("null")
    String getFilterType();

    @Config("column")
    @ConfigDefault("null")
    String getColumn();

    @Config("value")
    @ConfigDefault("null")
    Optional<Object> getValue();

    @Config("in_values")
    @ConfigDefault("null")
    Optional<List<Object>> getInValues();

}
