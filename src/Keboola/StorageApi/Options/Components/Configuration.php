<?php

namespace Keboola\StorageApi\Options\Components;

class Configuration
{
    private $componentId;

    private $configurationId;

    private $configuration;

    private $name;

    private $description;

    private $state;

    private $changeDescription;

    /** @var bool|null */
    private $isDisabled;

    private $rowsSortOrder = [];

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

    /**
     * @return string
     */
    public function getChangeDescription()
    {
        return $this->changeDescription;
    }

    /**
     * @param string $changeDescription
     * @return $this
     */
    public function setChangeDescription($changeDescription): static
    {
        $this->changeDescription = $changeDescription;
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
     * @return array
     */
    public function getRowsSortOrder()
    {
        return $this->rowsSortOrder;
    }

    /**
     * @param array $rowsSortOrder
     * @return $this
     */
    public function setRowsSortOrder(array $rowsSortOrder): static
    {
        $this->rowsSortOrder = $rowsSortOrder;
        return $this;
    }
}
