<?php

namespace Keboola\StorageApi\Options;

class TableWithConfigurationOptions
{
    private string $tablename;
    private string $configurationId;

    /**
     * @return string
     */
    public function getTablename(): string
    {
        return $this->tablename;
    }

    /**
     * @param string $tablename
     */
    public function setTablename(string $tablename): void
    {
        $this->tablename = $tablename;
    }

    /**
     * @return string
     */
    public function getConfigurationId(): string
    {
        return $this->configurationId;
    }

    /**
     * @param string $configurationId
     */
    public function setConfigurationId(string $configurationId): void
    {
        $this->configurationId = $configurationId;
    }
}
