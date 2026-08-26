<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\GcsClientFactory;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

class GcsClientFactoryTest extends TestCase
{
    /**
     * Guzzle's cURL handler ignores read_timeout, so the stall detection that actually applies
     * is cURL's low-speed abort. Without it a download that hangs mid-transfer never returns:
     * the google-cloud REST client sets no limits of its own.
     */
    #[RequiresPhpExtension('curl')]
    public function testDownloadOptionsAbortStalledTransfers(): void
    {
        $restOptions = GcsClientFactory::downloadOptions()['restOptions'];

        self::assertSame(
            [
                CURLOPT_LOW_SPEED_LIMIT => 1024,
                CURLOPT_LOW_SPEED_TIME => 60,
            ],
            $restOptions['curl'],
        );
        self::assertSame(60, $restOptions['read_timeout']);
        self::assertSame(10, $restOptions['connect_timeout']);
    }

    /**
     * A tight total request timeout caps a download by object size rather than by connection
     * health. The deadline that remains is only a liveness backstop for a transfer crawling just
     * above the stall threshold, and must stay far above any real export.
     */
    public function testTotalTransferDeadlineCannotBeReachedByALargeButHealthyDownload(): void
    {
        $timeout = GcsClientFactory::downloadOptions()['restOptions']['timeout'];

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
     * because neither factory owns the other.
     */
    public function testTransferPolicyMatchesTheAwsOne(): void
    {
        $gcs = GcsClientFactory::downloadOptions()['restOptions'];
        $aws = S3ClientFactory::transferOptions(Client::DEFAULT_RETRIES_COUNT)['http'];

        self::assertSame($aws['connect_timeout'], $gcs['connect_timeout']);
        self::assertSame($aws['timeout'], $gcs['timeout']);
        self::assertSame($aws['read_timeout'], $gcs['read_timeout']);
        self::assertSame($aws['curl'], $gcs['curl']);
    }
}
