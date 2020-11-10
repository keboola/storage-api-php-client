<?php

namespace Keboola\Test\Backend\FileWorkspace\Backend;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

class Abs
{
    /** @var string */
    private $connectionString;

    /** @var mixed */
    private $container;

    public function __construct(array $connection)
    {
        $this->connectionString = $connection['connectionString'];
        $this->container = $connection['container'];
    }

    /**
     * @return Blob[]
     */
    public function listFiles()
    {
        $blobs = $this->getClient()->listBlobs($this->container);
        return $blobs->getBlobs();
    }

    /**
     * @return BlobRestProxy
     */
    public function getClient()
    {
        $blobClient = BlobRestProxy::createBlobService($this->connectionString);
        $blobClient->pushMiddleware(RetryMiddlewareFactory::create());
        return $blobClient;
    }

    /**
     * @return string
     */
    public function uploadTestingFile()
    {
        $filePath = tempnam(sys_get_temp_dir(), 'abs-file-upload');
        file_put_contents($filePath, 'We fight for data');
        $content = fopen($filePath, "rb");
        $this->getClient()->createBlockBlob($this->container, basename($filePath), $content);

        return basename($filePath);
    }
}
