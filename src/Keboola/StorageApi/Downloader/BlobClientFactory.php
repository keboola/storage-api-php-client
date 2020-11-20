<?php

namespace Keboola\StorageApi\Downloader;

use Keboola\FileStorage\Abs\RetryMiddlewareFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

class BlobClientFactory
{
    /**
     * @param string $connectionString
     * @return BlobRestProxy
     */
    public static function createClientFromConnectionString(
        $connectionString
    ) {
        $client = BlobRestProxy::createBlobService($connectionString);
        $client->pushMiddleware(RetryMiddlewareFactory::create());

        return $client;
    }
}
