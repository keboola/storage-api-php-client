<?php

namespace Keboola\StorageApi;

use Aws\S3\S3Client;
use GuzzleHttp\Promise\Promise;
use Keboola\StorageApi\ABSUploader\PromiseHelper;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\CommitBlobBlocksOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;
use MicrosoftAzure\Storage\Common\Models\ServiceOptions;

class ABSUploader
{
    const CHUNK_SIZE = 4 * 1024 * 1024; // 4MiB
    const MAX_PARALLEL_CHUNKS = 125;
    const PADLENGTH = 5; // Size of the string used for the block ID, modify if needed.

    /** @var BlobRestProxy */
    private $blobClient;

    /**
     * @param S3Client $s3Client
     */
    public function __construct(BlobRestProxy $blobClient)
    {
        $this->blobClient = $blobClient;
    }

    /**
     * @param string $container
     * @param string $blobName
     * @param string $file
     * @param CommitBlobBlocksOptions|CreateBlockBlobOptions|null $options
     * @param bool $parallel
     */
    public function uploadFile($container, $blobName, $file, $options, $parallel)
    {
        if ($parallel) {
            if (!$options instanceof CommitBlobBlocksOptions) {
                throw new ClientException('Parallel upload needs CommitBlobBlocksOptions');
            }
        } else {
            if (!$options instanceof CreateBlockBlobOptions) {
                throw new ClientException('Single thread upload needs CreateBlockBlobOptions');
            }
        }
        $this->uploadAsync($container, $file, $blobName, $parallel, $options)->wait();
    }

    /**
     * @param string $container
     * @param string $file
     * @param string $blobName
     * @param bool $parallel
     * @param CommitBlobBlocksOptions|CreateBlockBlobOptions|null $options
     * @return Promise
     */
    private function uploadAsync($container, $file, $blobName, $parallel, $options = null)
    {
        if (!$parallel) {
            $promise = new Promise(\Closure::bind(function () use (&$promise, $container, $blobName, $file, $options) {
                $promise->resolve($this->blobClient->createBlockBlob(
                    $container,
                    $blobName,
                    fopen($file, 'r'),
                    $options
                ));
            }, $this));
            return $promise;
        }

        if ($options === null) {
            $options = new CommitBlobBlocksOptions();
        }
        $promise = new Promise(\Closure::bind(function () use (&$promise, $container, $blobName, $file, $options) {
            list($promises, $blockIds) = $this->uploadByBlocks($container, $blobName, $file);
            PromiseHelper::all($promises);
            $promise->resolve($this->blobClient->commitBlobBlocks($container, $blobName, $blockIds, $options));
        }, $this));
        return $promise;
    }

    private function uploadByBlocks($container, $blobName, $file)
    {
        /** @var resource $handle */
        $handle = fopen($file, 'rb');
        $blockIds = [];
        $counter = 1;
        $promises = [];
        $currentChunksUploading = 0;
        while ($data = fread($handle, self::CHUNK_SIZE)) {
            $blockId = base64_encode(str_pad((string) $counter, self::PADLENGTH, '0', STR_PAD_LEFT));
            $block = new \MicrosoftAzure\Storage\Blob\Models\Block();
            $block->setBlockId($blockId);
            $block->setType('Uncommitted');
            $blockIds[] = $block;
            // Upload the block.
            $promises[] = $this->blobClient->createBlobBlockAsync($container, $blobName, $blockId, $data);
            $counter++;
            $currentChunksUploading++;
            if ($currentChunksUploading === self::MAX_PARALLEL_CHUNKS) {
                // as all chunks are read asynchronously at same time a lot of memory is used during upload
                // ex. 10GiB file is chunked to 2500 chunks even few KiB read will cause big memory peak
                // this will wait for chunks upload after certain limit
                PromiseHelper::all($promises);
                $promises = [];
                $currentChunksUploading = 0;
            }
        }
        fclose($handle);
        // wait for promisse
        return [$promises, $blockIds];
    }

    /**
     * @param string $container
     * @param string $blobPrefix
     * @param string[] $slices
     */
    public function uploadSlicedFile($container, $blobPrefix, $slices)
    {
        foreach ($slices as $slice) {
            $parallel = true;
            if (filesize($slice) === 0) {
                $parallel = false;
            }
            $blobName = sprintf(
                '%s%s',
                $blobPrefix,
                basename($slice)
            );
            $promise = $this->uploadAsync($container, $slice, $blobName, $parallel);
            // wait for slice to upload
            $promise->wait();
        }
    }
}
