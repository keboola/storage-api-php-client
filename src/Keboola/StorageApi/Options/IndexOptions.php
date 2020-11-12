<?php

namespace Keboola\StorageApi\Options;

class IndexOptions
{
    /** @var array */
    private $exclude = [];

    /**
     * @return array
     */
    public function toArray()
    {
        return array(
            'exclude' => $this->getExcludeAsString(),
        );
    }

    /**
     * @return string
     */
    public function getExcludeAsString()
    {
        return implode(',', $this->getExclude());
    }

    /**
     * @return array
     */
    public function getExclude()
    {
        return $this->exclude;
    }

    /**
     * @param array $exclude
     */
    public function setExclude(array $exclude)
    {
        $this->exclude = $exclude;
        return $this;
    }
}
