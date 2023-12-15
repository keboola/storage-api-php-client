<?php

namespace Keboola\StorageApi\Downloader;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;

class AbsDownloader implements DownloaderInterface
{
    /**
     * @var BlobRestProxy
     */
    private $client;

    public function __construct(BlobRestProxy $client)
    {
        $this->client = $client;
    }

    /**
     * @inheritDoc
     */
    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath)
    {
        $result = $this->client->getBlob(
            $fileResponse['absPath']['container'],
            $fileResponse['absPath']['name'],
        );
        file_put_contents($tmpFilePath, $result->getContentStream());
    }

    /**
     * @inheritDoc
     */
    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath)
    {
        list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($entry['url']);

        $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $file));

        $result = $this->client->getBlob(
            $container,
            $file,
        );
        file_put_contents($filePath, $result->getContentStream());
        return $filePath;
    }
}
