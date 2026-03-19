<?php

namespace Keboola\StorageApi\Downloader;

interface DownloaderInterface
{
    /**
     * @param array $fileResponse
     * @param string $tmpFilePath
     * @return void
     */
    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath);

    /**
     * @param array $fileResponse
     * @param array $entry
     * @param string $tmpFilePath
     * @return string
     */
    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath);

    public function getEntryKey(string $entryUrl): string;
}
