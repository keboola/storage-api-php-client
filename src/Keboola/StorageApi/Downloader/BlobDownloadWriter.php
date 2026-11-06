<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Keboola\StorageApi\Exception;
use MicrosoftAzure\Storage\Blob\Models\GetBlobResult;
use Symfony\Component\Filesystem\Filesystem;

/**
 * Writes a fetched blob body to disk and refuses one shorter than the blob.
 *
 * @internal Not part of the published package API.
 */
final class BlobDownloadWriter
{
    public const INCOMPLETE_DOWNLOAD = 'incompleteDownload';

    public static function writeToFile(GetBlobResult $result, string $destination): void
    {
        $filesystem = new Filesystem();
        $filesystem->dumpFile($destination, $result->getContentStream());

        // Once its retries are spent, the Azure SDK fulfils a transfer whose connection dropped
        // mid-body with the last attempt's response, whose body is at EOF. The size on disk is the
        // only signal left that the blob is incomplete. A missing Content-Length (Guzzle drops it
        // for a Content-Encoding blob) reads as 0 and is not enforced.
        clearstatcache(true, $destination);
        $written = filesize($destination);
        $expected = $result->getProperties()->getContentLength();
        if ($written === false || $written < $expected) {
            $filesystem->remove($destination);
            throw new Exception(
                sprintf('Blob download is incomplete: expected %d bytes, got %d.', $expected, (int) $written),
                null,
                null,
                self::INCOMPLETE_DOWNLOAD,
                ['expectedBytes' => $expected, 'writtenBytes' => (int) $written],
            );
        }
    }
}
