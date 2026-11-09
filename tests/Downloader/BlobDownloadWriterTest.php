<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Downloader\BlobDownloadWriter;
use Keboola\StorageApi\Downloader\BlobStorageRetryMiddleware;
use Keboola\StorageApi\Exception;
use PHPUnit\Framework\TestCase;

class BlobDownloadWriterTest extends TestCase
{
    use StallingBlobServer;

    /**
     * The in-memory counterpart of AbsDownloaderTest::testAConnectionClosedMidBodyRaisesAndLeavesNoFile:
     * after the retries the SDK hands back a 200 result whose body is at EOF, and the string length
     * is the only signal left. Pins the same third-party behaviour as that test.
     */
    public function testAConnectionClosedMidBodyRaisesWhenReadIntoMemory(): void
    {
        $client = $this->createClientWithShortStallWindow($this->startStallingServer(0, 1024), true);
        $client->pushMiddleware(BlobStorageRetryMiddleware::create(
            2,
            10,
            BlobStorageRetryMiddleware::LINEAR_INTERVAL_ACCUMULATION,
        ));
        $result = $client->getBlob('container', 'manifest');

        try {
            BlobDownloadWriter::readToString($result);
            self::fail('a body shorter than its Content-Length must not be returned');
        } catch (Exception $e) {
            self::assertSame('incompleteDownload', $e->getStringCode());
            self::assertStringContainsString('expected 262144 bytes', $e->getMessage());
        }
    }

    /** Guards the check itself against a false positive: a complete body is returned whole. */
    public function testACompleteBodyIsReturnedInFull(): void
    {
        $client = $this->createClientWithShortStallWindow(
            $this->startStallingServer(0, self::BLOB_SIZE_BYTES),
            true,
        );

        self::assertSame(
            self::BLOB_SIZE_BYTES,
            strlen(BlobDownloadWriter::readToString($client->getBlob('container', 'manifest'))),
        );
    }
}
