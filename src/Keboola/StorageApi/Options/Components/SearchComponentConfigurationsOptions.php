<?php

namespace Keboola\StorageApi\Options\Components;

class SearchComponentConfigurationsOptions
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
    public function setComponentId($componentId): static
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
    public function setConfigurationId($configurationId): static
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
     * @return $this
     */
    public function setMetadataKeys($metadataKeys): static
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
     * @return $this
     */
    public function setInclude($include): static
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

    public function toParamsArray(): array
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
