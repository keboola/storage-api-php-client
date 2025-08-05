<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Exporter;

use Keboola\StorageApi\Downloader\DownloaderInterface;

class DownloadedSliceEntry
{
    public function __construct(
        public readonly string $entryUrl,
        public readonly string $tempFilePath,
        public readonly string $filePath,
    ) {
    }

    public function getFileName(DownloaderInterface $downloader): string
    {
        $entryKey = $downloader->getEntryKey($this->entryUrl);
        return basename($entryKey);
    }
}
