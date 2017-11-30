<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 23/01/14
 * Time: 15:02
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options;

use Keboola\StorageApi\ClientException;

class FileUploadTransferOptions
{

    private $chunkSize = 50;

    private $maxRetriesPerChunk = 50;

    /**
     * @return int
     */
    public function getChunkSize()
    {
        return $this->chunkSize;
    }

    /**
     * @param $chunkSize
     * @return $this
     * @throws ClientException
     */
    public function setChunkSize($chunkSize)
    {
        if ((int) $chunkSize <= 0) {
            throw new ClientException("Invalid chunk size: '{$chunkSize}'");
        }
        $this->chunkSize = (int) $chunkSize;
        return $this;
    }

    /**
     * @return int
     */
    public function getMaxRetriesPerChunk()
    {
        return $this->maxRetriesPerChunk;
    }

    /**
     * @param $maxRetriesPerChunk
     * @return $this
     * @throws ClientException
     */
    public function setMaxRetriesPerChunk($maxRetriesPerChunk)
    {
        if ((int) $maxRetriesPerChunk <= 0) {
            throw new ClientException("Invalid max retries per chunk size: '{$maxRetriesPerChunk}'");
        }
        $this->maxRetriesPerChunk = (int) $maxRetriesPerChunk;


        $this->maxRetriesPerChunk = (int) $maxRetriesPerChunk;
        return $this;
    }


}
