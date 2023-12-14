<?php

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;

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
                    'retries' => 40,
                    'credentials' => [
                        'key' => $getFileResponse['credentials']['AccessKeyId'],
                        'secret' => $getFileResponse['credentials']['SecretAccessKey'],
                        'token' => $getFileResponse['credentials']['SessionToken'],
                    ],
                    'http' => [
                        'read_timeout' => 10,
                        'decode_content' => false,
                        'connect_timeout' => 10,
                        'timeout' => 500,
                    ],
                ]);
                return new S3Downloader($s3Client);
            case Client::FILE_PROVIDER_AZURE:
                $blobClient = BlobClientFactory::createClientFromConnectionString(
                    $getFileResponse['absCredentials']['SASConnectionString'],
                );
                return new AbsDownloader($blobClient);
            case Client::FILE_PROVIDER_GCP:
                $gcsClient = GcsClientFactory::createClientFromCredentialsArray(
                    $getFileResponse,
                );
                return new GcsDownloader($gcsClient);
        }

        throw new Exception(sprintf(
            'There is no downloader implemented for "%s" provider.',
            $getFileResponse['provider'],
        ));
    }
}
