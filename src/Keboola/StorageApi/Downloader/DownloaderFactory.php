<?php

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

class DownloaderFactory
{
    /**
     * @param array $getFileResponse
     * @param int $retries
     * @return DownloaderInterface
     * @throws Exception
     */
    public static function createDownloaderForFileResponse($getFileResponse, $retries = Client::DEFAULT_RETRIES_COUNT)
    {
        switch ($getFileResponse['provider']) {
            case Client::FILE_PROVIDER_AWS:
                $s3Client = new S3Client([
                    'version' => '2006-03-01',
                    'region' => $getFileResponse['region'],
                    'retries' => $retries,
                    'credentials' => [
                        'key' => $getFileResponse["credentials"]["AccessKeyId"],
                        'secret' => $getFileResponse["credentials"]["SecretAccessKey"],
                        'token' => $getFileResponse["credentials"]["SessionToken"],
                    ],
                    'http' => [
                        'decode_content' => false,
                    ],
                ]);
                return new S3Downloader($s3Client);
            case Client::FILE_PROVIDER_AZURE:
                $blobClient = BlobRestProxy::createBlobService(
                    $getFileResponse['absCredentials']['SASConnectionString']
                );
                $blobClient->pushMiddleware(RetryMiddlewareFactory::create());
                return new AbsDownloader($blobClient);
        }

        throw new Exception(sprintf(
            'There is no downloader implemented for "%s" provider.',
            $getFileResponse['provider']
        ));
    }
}
