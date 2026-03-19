<?php
namespace Keboola\StorageApi\Options\Components;

class ConfigurationRow
{
    private $rowId;

    private $configuration;

    private $changeDescription;

    private $name;

    private $description;

    private $isDisabled;

    private $state;

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
    public function setRowId($rowId): static
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
    public function setConfiguration($configuration): static
    {
        $this->configuration = (array) $configuration;
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
     * @return $this
     */
    public function setChangeDescription($changeDescription): static
    {
        $this->changeDescription = $changeDescription;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param mixed $name
     * @return $this
     */
    public function setName($name): static
    {
        $this->name = $name;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getDescription()
    {
        return $this->description;
    }

    /**
     * @param mixed $description
     * @return $this
     */
    public function setDescription($description): static
    {
        $this->description = $description;
        return $this;
    }

    /**
     * @return bool|null
     */
    public function getIsDisabled()
    {
        return $this->isDisabled;
    }

    /**
     * @param bool $isDisabled
     * @return $this
     */
    public function setIsDisabled($isDisabled): static
    {
        $this->isDisabled = (bool) $isDisabled;
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
