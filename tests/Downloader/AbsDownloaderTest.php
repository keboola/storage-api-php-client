<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Downloader\AbsDownloader;
use Keboola\StorageApi\Downloader\BlobDownloadWriter;
use Keboola\StorageApi\Downloader\BlobStorageRetryMiddleware;
use Keboola\StorageApi\Exception;
use PHPUnit\Framework\TestCase;

class AbsDownloaderTest extends TestCase
{
    use StallingBlobServer;

    /**
     * A connection closed mid-body is not a stall: cURL reports it as errno 18, Guzzle attaches the
     * 200 response to the rejection, and once BlobStorageRetryMiddleware gives up the SDK returns
     * that response as a success with its body at EOF. Without the size check the downloader
     * writes an empty file and returns.
     *
     * This pins third-party behaviour: if an SDK or Guzzle release ever makes such a transfer
     * raise on its own, getBlob() throws before the downloader runs and this test starts failing.
     * Relax it to "raises" then, do not work around it.
     */
    public function testAConnectionClosedMidBodyRaisesAndLeavesNoFile(): void
    {
        $client = $this->createClientWithShortStallWindow($this->startStallingServer(0, 1024), true);
        $client->pushMiddleware(BlobStorageRetryMiddleware::create(
            2,
            10,
            BlobStorageRetryMiddleware::LINEAR_INTERVAL_ACCUMULATION,
        ));
        $destination = tempnam(sys_get_temp_dir(), 'abs-download-');
        self::assertIsString($destination);

        try {
            (new AbsDownloader($client))->downloadFileFromFileResponse(
                ['absPath' => ['container' => 'container', 'name' => 'blob']],
                $destination,
            );
            self::fail('a blob shorter than its Content-Length must not be reported as downloaded');
        } catch (Exception $e) {
            self::assertSame(BlobDownloadWriter::INCOMPLETE_DOWNLOAD, $e->getStringCode());
            self::assertStringContainsString('expected 262144 bytes', $e->getMessage());
        } finally {
            $leftover = file_exists($destination);
            if ($leftover) {
                unlink($destination);
            }
        }

        self::assertFalse($leftover, 'a short download must not leave a file behind');
    }

    /** Guards the check itself against a false positive: a complete body is written as is. */
    public function testACompleteBodyIsWrittenInFull(): void
    {
        $client = $this->createClientWithShortStallWindow(
            $this->startStallingServer(0, self::BLOB_SIZE_BYTES),
            true,
        );
        $destination = tempnam(sys_get_temp_dir(), 'abs-download-');
        self::assertIsString($destination);

        try {
            (new AbsDownloader($client))->downloadFileFromFileResponse(
                ['absPath' => ['container' => 'container', 'name' => 'blob']],
                $destination,
            );

            self::assertSame(self::BLOB_SIZE_BYTES, filesize($destination));
        } finally {
            if (file_exists($destination)) {
                unlink($destination);
            }
        }
    }
}
