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

    /**
     *
     * Files in a chunk (for parallel multipart upload)
     *
     * @var int
     */
    private $chunkSize = 50;

    /**
     *
     * Max retries of multipart uploads in a chunk
     *
     * @var int
     */
    private $maxRetriesPerChunk = 10;

    /**
     *
     * Multipart upload threads for single file uploads
     *
     * @var int
     */
    private $singleFileConcurrency = 20;

    /**
     *
     * Multipart upload threads for multiple files uploads
     *
     * @var int
     */
    private $multiFileConcurrency = 5;

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
    public function setChunkSize($chunkSize): static
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
    public function setMaxRetriesPerChunk($maxRetriesPerChunk): static
    {
        if ((int) $maxRetriesPerChunk <= 0) {
            throw new ClientException("Invalid max retries per chunk: '{$maxRetriesPerChunk}'");
        }
        $this->maxRetriesPerChunk = (int) $maxRetriesPerChunk;
        return $this;
    }

    /**
     * @return int
     */
    public function getSingleFileConcurrency()
    {
        return $this->singleFileConcurrency;
    }

    /**
     * @param int $singleFileConcurrency
     * @return $this
     * @throws ClientException
     */
    public function setSingleFileConcurrency($singleFileConcurrency): static
    {
        if ((int) $singleFileConcurrency <= 0) {
            throw new ClientException("Invalid single file concurrency: '{$singleFileConcurrency}'");
        }
        $this->singleFileConcurrency = (int) $singleFileConcurrency;
        return $this;
    }

    /**
     * @return int
     */
    public function getMultiFileConcurrency()
    {
        return $this->multiFileConcurrency;
    }

    /**
     * @param int $multiFileConcurrency
     * @return $this
     * @throws ClientException
     */
    public function setMultiFileConcurrency($multiFileConcurrency): static
    {
        if ((int) $multiFileConcurrency <= 0) {
            throw new ClientException("Invalid multi file concurrency: '{$multiFileConcurrency}'");
        }
        $this->multiFileConcurrency = (int) $multiFileConcurrency;
        return $this;
    }
}
