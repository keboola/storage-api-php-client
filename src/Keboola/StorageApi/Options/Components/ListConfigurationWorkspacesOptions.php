<?php
namespace Keboola\StorageApi\Options\Components;

class ListConfigurationWorkspacesOptions
{
    private $componentId;

    private $configurationId;

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
    public function setComponentId($componentId): static
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
    public function setConfigurationId($configurationId): static
    {
        $this->configurationId = $configurationId;
        return $this;
    }
}
