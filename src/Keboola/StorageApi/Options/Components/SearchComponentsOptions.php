<?php

namespace Keboola\StorageApi\Options\Components;

class SearchComponentsOptions
{
    private $componentId;

    private $configurationId;

    private $metadataKeys;

    private $include;

    /**
     * @return string
     */
    public function getComponentId()
    {
        return $this->componentId;
    }

    /**
     * @param string $componentId
     * @return $this
     */
    public function setComponentId($componentId)
    {
        $this->componentId = $componentId;
        return $this;
    }

    /**
     * @return string
     */
    public function getConfigurationId()
    {
        return $this->configurationId;
    }

    /**
     * @param string $configurationId
     * @return $this
     */
    public function setConfigurationId($configurationId)
    {
        $this->configurationId = $configurationId;
        return $this;
    }

    /**
     * @return array
     */
    public function getMetadataKeys()
    {
        return $this->metadataKeys;
    }

    /**
     * @param array $metadataKeys
     */
    public function setMetadataKeys($metadataKeys)
    {
        $this->metadataKeys = $metadataKeys;
        return $this;
    }

    /**
     * @return array
     */
    public function getInclude()
    {
        return $this->include;
    }

    /**
     * @param array $include
     */
    public function setInclude($include)
    {
        $this->include = $include;
        return $this;
    }

    /**
     * @return string
     */
    public function getIncludeAsString()
    {
        return implode(',', $this->getInclude());
    }

    public function toParamsArray()
    {
        $params = [];

        if ($this->getComponentId()) {
            $params['idComponent'] = $this->getComponentId();
        }

        if ($this->getConfigurationId()) {
            $params['configurationId'] = $this->getConfigurationId();
        }

        if ($this->getMetadataKeys()) {
            $params['metadataKeys'] = $this->getMetadataKeys();
        }

        if ($this->getInclude()) {
            $params['include'] = $this->getIncludeAsString();
        }

        return $params;
    }
}
