<?php

namespace Keboola\StorageApi\Options;

class SearchTablesOptions
{
    /** @var string|null */
    private $metadataKey;

    /** @var string|null */
    private $metadataValue;

    /** @var string|null */
    private $metadataProvider;

    /**
     * @param string|null $metadataKey
     * @param string|null $metadataValue
     * @param string|null $metadataProvider
     * @return SearchTablesOptions
     */
    public static function create($metadataKey, $metadataValue, $metadataProvider)
    {
        $self = new self();
        $self->metadataKey = $metadataKey;
        $self->metadataValue = $metadataValue;
        $self->metadataProvider = $metadataProvider;
        return $self;
    }

    /**
     * @param string|null $metadataKey
     * @return SearchTablesOptions
     */
    public function setMetadataKey($metadataKey)
    {
        $this->metadataKey = $metadataKey;
        return $this;
    }

    /**
     * @param string|null $metadataValue
     * @return SearchTablesOptions
     */
    public function setMetadataValue($metadataValue)
    {
        $this->metadataValue = $metadataValue;
        return $this;
    }

    /**
     * @param string|null $metadataProvider
     * @return SearchTablesOptions
     */
    public function setMetadataProvider($metadataProvider)
    {
        $this->metadataProvider = $metadataProvider;
        return $this;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return [
            'metadataKey' => $this->metadataKey,
            'metadataValue' => $this->metadataValue,
            'metadataProvider' => $this->metadataProvider,
        ];
    }
}
