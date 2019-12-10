<?php

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

class DownloaderFactory
{
    /**
     * @param $fileInfo
     * @param int $retries
     * @return DownloaderInterface
     * @throws Exception
     */
    public static function createDownloaderForPrepareRequest($fileInfo, $retries = Client::DEFAULT_RETRIES_COUNT)
    {
        switch ($fileInfo['provider']) {
            case Client::FILE_PROVIDER_AWS:
                $s3Client = new S3Client([
                    'version' => '2006-03-01',
                    'region' => $fileInfo['region'],
                    'retries' => $retries,
                    'credentials' => [
                        'key' => $fileInfo["credentials"]["AccessKeyId"],
                        'secret' => $fileInfo["credentials"]["SecretAccessKey"],
                        'token' => $fileInfo["credentials"]["SessionToken"],
                    ],
                    'http' => [
                        'decode_content' => false,
                    ],
                ]);
                return new S3Downloader($s3Client);
            case Client::FILE_PROVIDER_AZURE:
                $blobClient = BlobRestProxy::createBlobService(
                    $fileInfo['absCredentials']['SASConnectionString']
                );
                return new AbsDownloader($blobClient);
        }

        throw new Exception(sprintf(
            'There is no downloader implemented for "%s" provider.',
            $fileInfo['provider']
        ));
    }
}
