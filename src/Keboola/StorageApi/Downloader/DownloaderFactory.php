<?php

namespace Keboola\StorageApi\Downloader;

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
                return new S3Downloader(S3ClientFactory::createClient(
                    $getFileResponse['region'],
                    $getFileResponse['credentials']['AccessKeyId'],
                    $getFileResponse['credentials']['SecretAccessKey'],
                    $getFileResponse['credentials']['SessionToken'],
                    (int) $retries,
                ));
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
