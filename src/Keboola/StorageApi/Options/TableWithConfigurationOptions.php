<?php

namespace Keboola\StorageApi\Options;

class TableWithConfigurationOptions
{
    private string $tableName;
    private string $configurationId;

    public function getTableName(): string
    {
        return $this->tableName;
    }


    public function setTableName(string $tableName): self
    {
        $this->tableName = $tableName;
        return $this;
    }

    public function getConfigurationId(): string
    {
        return $this->configurationId;
    }

    public function setConfigurationId(string $configurationId): self
    {
        $this->configurationId = $configurationId;
        return $this;
    }
}
