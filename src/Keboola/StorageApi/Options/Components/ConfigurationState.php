<?php
namespace Keboola\StorageApi\Options\Components;

class ConfigurationState
{
    private $componentId;

    private $configurationId;

    private $state;

    /**
     * @return mixed
     */
    public function getComponentId()
    {
        return $this->componentId;
    }

    /**
     * @param mixed $componentId
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
     */
    public function setConfigurationId($configurationId)
    {
        $this->configurationId = $configurationId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getState()
    {
        return $this->state;
    }

    /**
     * @param mixed $state
     */
    public function setState($state)
    {
        $this->state = $state;
        return $this;
    }
}
