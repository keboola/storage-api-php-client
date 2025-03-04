<?php
namespace Keboola\StorageApi\Options\Components;

class ConfigurationRowState
{
    private $rowId;

    private $state;

    /** @var Configuration */
    private $componentConfiguration;

    public function __construct(Configuration $configuration)
    {
        $this->componentConfiguration = $configuration;
    }

    /** @return Configuration */
    public function getComponentConfiguration()
    {
        return $this->componentConfiguration;
    }

    /**
     * @return mixed
     */
    public function getRowId()
    {
        return $this->rowId;
    }

    /**
     * @param mixed $rowId
     * @return $this
     */
    public function setRowId($rowId): static
    {
        $this->rowId = $rowId;
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
     * @return $this
     */
    public function setState($state): static
    {
        $this->state = $state;
        return $this;
    }
}
