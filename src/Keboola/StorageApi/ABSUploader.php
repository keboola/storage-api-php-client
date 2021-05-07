<?php

namespace Keboola\StorageApi;

use Aws\S3\S3Client;
use GuzzleHttp\Promise\Promise;
use Keboola\StorageApi\ABSUploader\PromiseHelper;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;

class ABSUploader
{
    const CHUNK_SIZE = 4 * 1024 * 1024;
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
     * @param CreateBlockBlobOptions $options
     */
    public function uploadFile($container, $blobName, $file, $options)
    {
        $this->uploadAsync($container, $blobName, $file, $options)->wait();
    }

    /**
     * @param string $container
     * @param string $blobName
     * @param string $file
     * @param CreateBlockBlobOptions|null $options
     * @return Promise
     */
    private function uploadAsync($container, $blobName, $file, $options = null)
    {
        if ($options === null) {
            $options = new CreateBlockBlobOptions();
        }
        $promise = new Promise(\Closure::bind(function () use (&$promise, $container, $blobName, $file, $options) {
            list($promises, $blockIds) = $this->uploadByBlocks($container, $blobName, $file);
            PromiseHelper::all($promises);
            $promise->resolve($this->blobClient->commitBlobBlocks($container, $blobName, $blockIds/*, $options*/));
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
        while (!feof($handle)) {
            $blockId = base64_encode(str_pad($counter, self::PADLENGTH, '0', STR_PAD_LEFT));
            $block = new \MicrosoftAzure\Storage\Blob\Models\Block();
            $block->setBlockId($blockId);
            $block->setType('Uncommitted');
            $blockIds[] = $block;
            $data = fread($handle, self::CHUNK_SIZE);
            // Upload the block.
            $promises[] = $this->blobClient->createBlobBlockAsync($container, $blobName, $blockId, $data);
            $counter++;
        }
        fclose($handle);
        // wait for promisse
        return [$promises, $blockIds];
    }

    /**
     * @param string $container
     * @param string $blobName
     * @param string[] $slices
     */
    public function uploadSlicedFile($container, $blobName, $slices)
    {
        $promises = [];
        foreach ($slices as $slice) {
            $promises[] = $this->uploadAsync($container, $blobName, $slice);
        }
        PromiseHelper::all($promises);
    }
}
