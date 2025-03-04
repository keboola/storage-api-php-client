<?php
/**
 * @package storage-api-php-client
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\StorageApi\Options\Components;

class ListConfigurationVersionsOptions
{
    private $componentId;

    private $configurationId;

    private $include = [];

    private $offset;

    private $limit;

    public function toParamsArray()
    {
        return [
            'include' => implode(',', $this->getInclude()),
            'offset' => $this->getOffset(),
            'limit' => $this->getLimit(),
        ];
    }

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
    public function getOffset()
    {
        return $this->offset;
    }

    /**
     * @param mixed $offset
     * @return $this
     */
    public function setOffset($offset): static
    {
        $this->offset = $offset;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     * @param mixed $limit
     * @return $this
     */
    public function setLimit($limit): static
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * @param array $include
     * @return $this
     */
    public function setInclude($include = []): static
    {
        $this->include = (array) $include;
        return $this;
    }

    public function getInclude()
    {
        return $this->include;
    }
}
