<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Throwable;

/**
 * @internal Not part of the published package API.
 */
final class FailureTranslatingDownloader implements DownloaderInterface
{
    public function __construct(private readonly DownloaderInterface $downloader)
    {
    }

    /**
     * @param array<mixed> $fileResponse
     * @param string $tmpFilePath
     * @return void
     */
    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath)
    {
        try {
            $this->downloader->downloadFileFromFileResponse($fileResponse, $tmpFilePath);
        } catch (Throwable $e) {
            throw TransportFailure::translate($e, $fileResponse);
        }
    }

    /**
     * @param array<mixed> $fileResponse
     * @param array<mixed> $entry
     * @param string $tmpFilePath
     * @return string
     */
    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath)
    {
        try {
            return $this->downloader->downloadManifestEntry($fileResponse, $entry, $tmpFilePath);
        } catch (Throwable $e) {
            throw TransportFailure::translate($e, $fileResponse);
        }
    }

    public function getEntryKey(string $entryUrl): string
    {
        return $this->downloader->getEntryKey($entryUrl);
    }
}
