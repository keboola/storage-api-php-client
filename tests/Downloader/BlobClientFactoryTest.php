<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use PHPUnit\Framework\TestCase;

class BlobClientFactoryTest extends TestCase
{
    /**
     * The Azure SDK requests blob bodies as streams, which Guzzle's default handler routes to its
     * StreamHandler; there a stalled socket ends the copy silently with a truncated file. The
     * download client therefore forces the cURL handler, where cURL's low-speed abort turns a
     * stall into an exception the SDK retry middleware can act on.
     */
    public function testDownloadOptionsAbortStalledTransfers(): void
    {
        $options = BlobClientFactory::downloadTransferOptions();

        self::assertSame(
            [
                CURLOPT_LOW_SPEED_LIMIT => 1024,
                CURLOPT_LOW_SPEED_TIME => 60,
            ],
            $options['curl'],
        );
        self::assertSame(10, $options['connect_timeout']);
    }

    /**
     * Under the cURL handler `timeout` is a total request deadline, so it must stay far above any
     * real export: it is only a liveness backstop for a transfer crawling just above the stall
     * threshold, never a size cap.
     */
    public function testTotalTransferDeadlineCannotBeReachedByALargeButHealthyDownload(): void
    {
        $timeout = BlobClientFactory::downloadTransferOptions()['timeout'];

        self::assertSame(12 * 3600, $timeout);

        $bytesAtTypicalThroughput = 80 * 1024 * 1024 * $timeout;
        self::assertGreaterThan(
            1024 ** 4,
            $bytesAtTypicalThroughput,
            'the deadline must allow well over a terabyte at a normal transfer rate',
        );
    }

    /**
     * The providers deliberately share one transfer policy; the constants are duplicated only
     * because none of the factories owns the others.
     */
    public function testTransferPolicyMatchesTheAwsOne(): void
    {
        $azure = BlobClientFactory::downloadTransferOptions();
        $aws = S3ClientFactory::transferOptions(Client::DEFAULT_RETRIES_COUNT)['http'];

        self::assertSame($aws['connect_timeout'], $azure['connect_timeout']);
        self::assertSame($aws['timeout'], $azure['timeout']);
        self::assertSame($aws['curl'], $azure['curl']);
    }
}
