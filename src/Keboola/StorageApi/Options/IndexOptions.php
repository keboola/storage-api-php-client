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
        return [
            'exclude' => $this->getExcludeAsString(),
        ];
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
     * @return $this
     */
    public function setExclude(array $exclude): static
    {
        $this->exclude = $exclude;
        return $this;
    }
}
