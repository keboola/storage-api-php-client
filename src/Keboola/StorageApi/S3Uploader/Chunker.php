<?php

namespace Keboola\StorageApi\S3Uploader;

class Chunker
{
    /**
     * @var int
     */
    protected $size;

    /**
     * Chunker constructor.
     * @param int $size
     */
    public function __construct($size = 50)
    {
        $this->setSize($size);
    }

    /**
     * @return int
     */
    public function getSize()
    {
        return $this->size;
    }

    /**
     * @param int $size
     * @return $this
     */
    public function setSize($size)
    {
        $this->size = (int) $size;
        return $this;
    }

    /**
     * @param array $items
     * @return array
     */
    public function makeChunks($items)
    {
        $chunkCount = ceil(count($items) / $this->getSize());
        $chunks = [];
        for ($i = 0; $i < $chunkCount; $i++) {
            $currentChunk = array_slice(
                $items,
                $i * $this->getSize(),
                $this->getSize(),
            );
            $chunks[] = $currentChunk;
        }
        return $chunks;
    }
}
