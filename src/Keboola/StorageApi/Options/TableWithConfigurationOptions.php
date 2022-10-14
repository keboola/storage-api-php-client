<?php

namespace Keboola\StorageApi\Options;

class TableWithConfigurationOptions
{
    private string $tableName;

    private string $configurationId;

    public function __construct(
        string $tableName,
        string $configurationId
    ) {
        $this->tableName = $tableName;
        $this->configurationId = $configurationId;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getConfigurationId(): string
    {
        return $this->configurationId;
    }
}
