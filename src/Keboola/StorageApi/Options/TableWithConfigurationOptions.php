<?php

namespace Keboola\StorageApi\Options;

class TableWithConfigurationOptions
{
    private string $tablename;
    private string $configurationId;

    public function getTablename(): string
    {
        return $this->tablename;
    }


    public function setTablename(string $tablename): self
    {
        $this->tablename = $tablename;
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
