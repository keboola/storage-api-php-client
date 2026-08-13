<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\DownloaderFactory;
use Keboola\StorageApi\Downloader\S3Downloader;
use Keboola\StorageApi\Exception;
use PHPUnit\Framework\TestCase;

class DownloaderFactoryTest extends TestCase
{
    public function testAwsProviderReturnsS3Downloader(): void
    {
        $downloader = DownloaderFactory::createDownloaderForFileResponse([
            'provider' => Client::FILE_PROVIDER_AWS,
            'region' => 'eu-central-1',
            'credentials' => [
                'AccessKeyId' => 'access-key-id',
                'SecretAccessKey' => 'secret-access-key',
                'SessionToken' => 'session-token',
            ],
        ]);

        self::assertInstanceOf(S3Downloader::class, $downloader);
    }

    public function testUnknownProviderThrows(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('There is no downloader implemented for "unknown" provider.');

        DownloaderFactory::createDownloaderForFileResponse(['provider' => 'unknown']);
    }
}
