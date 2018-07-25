<?php
namespace Keboola\StorageApi\Options\Components;

class ListConfigurationRowsOptions
{
    private $componentId;

    private $configurationId;

    private $rowId;

    /**
     * @return mixed
     */
    public function getComponentId()
    {
        return $this->componentId;
    }

    /**
     * @param mixed $componentId
     * @return $this
     */
    public function setComponentId($componentId)
    {
        $this->componentId = $componentId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getConfigurationId()
    {
        return $this->configurationId;
    }

    /**
     * @param mixed $configurationId
     * @return $this
     */
    public function setConfigurationId($configurationId)
    {
        $this->configurationId = $configurationId;
        return $this;
    }

    public function getRowId()
    {
        return $this->rowId;
    }

    public function setRowId($rowId)
    {
        $this->rowId = $rowId;
        return $this;
    }
}
