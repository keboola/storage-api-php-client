<?php

namespace Keboola\StorageApi\Downloader;

use Google\Cloud\Storage\StorageClient;

class GcsDownloader implements DownloaderInterface
{
    /** @var StorageClient $client */
    private $client;

    public function __construct(StorageClient $client)
    {
        $this->client = $client;
    }

    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath)
    {
        $bucket = $this->client->bucket($fileResponse['gcsPath']['bucket']);
        $object = $bucket->object($fileResponse['gcsPath']['key']);
        $object->downloadToFile($tmpFilePath);
    }

    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath)
    {
        $fileKey = $this->getEntryKey($entry['url']);
        $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));

        $bucket = $this->client->bucket($fileResponse['gcsPath']['bucket']);
        $object = $bucket->object($fileKey);
        $object->downloadToFile($filePath);

        return $filePath;
    }

    public function getEntryKey(string $entryUrl): string
    {
        return substr($entryUrl, strpos($entryUrl, '/', 5) + 1);
    }
}
