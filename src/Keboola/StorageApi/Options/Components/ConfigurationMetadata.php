<?php

namespace Keboola\StorageApi\Options\Components;

class ConfigurationMetadata
{
    /** @var Configuration */
    private $componentConfiguration;

    /** @var array */
    private $metadata;

    public function __construct(
        Configuration $componentConfiguration
    ) {
        $this->componentConfiguration = $componentConfiguration;
    }

    /**
     * @return Configuration
     */
    public function getComponentConfiguration()
    {
        return $this->componentConfiguration;
    }

    /**
     * @return array
     */
    public function getMetadata()
    {
        return $this->metadata;
    }

    /**
     * @param array $metadata
     * @return $this
     */
    public function setMetadata($metadata): static
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new \LogicException('Argument must be a non-empty array of metadata objects');
        }

        $this->metadata = $metadata;
        return $this;
    }
}
