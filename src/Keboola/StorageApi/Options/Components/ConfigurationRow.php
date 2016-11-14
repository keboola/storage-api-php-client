<?php
namespace Keboola\StorageApi\Options\Components;

class ConfigurationRow
{
    private $rowId;

    private $configuration;

    private $changeDescription;

    /**
     * @var Configuration
     */
    private $componentConfiguration;

    public function __construct(Configuration $configuration)
    {
        $this->componentConfiguration = $configuration;
    }

    /**
     * @return Configuration
     */
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
    public function setRowId($rowId)
    {
        $this->rowId = $rowId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getConfiguration()
    {
        return $this->configuration;
    }

    /**
     * @param $configuration
     * @return $this
     */
    public function setConfiguration($configuration)
    {
        $this->configuration = (array)$configuration;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getChangeDescription()
    {
        return $this->changeDescription;
    }

    /**
     * @param mixed $changeDescription
     */
    public function setChangeDescription($changeDescription)
    {
        $this->changeDescription = $changeDescription;
        return $this;
    }
}
