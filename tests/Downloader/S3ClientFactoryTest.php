<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

class S3ClientFactoryTest extends TestCase
{
    private const REGION = 'eu-central-1';
    private const ACCESS_KEY_ID = 'access-key-id';
    private const SECRET_ACCESS_KEY = 'secret-access-key';
    private const SESSION_TOKEN = 'session-token';

    /**
     * @return array{
     *     version: string,
     *     retries: int,
     *     http: array{
     *         decode_content: bool,
     *         connect_timeout: int,
     *         timeout: int,
     *         read_timeout: int,
     *         curl: array<int, int>,
     *     },
     * }
     */
    private static function options(int $retries = Client::DEFAULT_RETRIES_COUNT): array
    {
        return S3ClientFactory::transferOptions($retries);
    }

    /**
     * The transfer policy is deliberately credential-free: it is reachable from anywhere, and a
     * failing assertion or a dump of the whole array must not be able to leak download credentials.
     */
    public function testTransferOptionsCarryNoCredentials(): void
    {
        $flattened = json_encode(self::options());
        self::assertIsString($flattened);

        self::assertArrayNotHasKey('credentials', self::options());
        foreach ([self::ACCESS_KEY_ID, self::SECRET_ACCESS_KEY, self::SESSION_TOKEN] as $secret) {
            self::assertStringNotContainsString($secret, $flattened);
        }
    }

    /**
     * A tight total request timeout caps a download by object size rather than by connection
     * health: at ~80 MB/s the previous 500s deadline made anything above ~40 GB undownloadable.
     * The deadline that remains is only a liveness backstop and must stay far above the largest
     * transfer any real export could need.
     */
    public function testTotalTransferDeadlineCannotBeReachedByALargeButHealthyDownload(): void
    {
        $timeout = self::options()['http']['timeout'];

        self::assertSame(S3ClientFactory::MAX_TRANSFER_SECONDS, $timeout);

        $bytesAtTypicalThroughput = 80 * 1024 * 1024 * $timeout;
        self::assertGreaterThan(
            1024 ** 4,
            $bytesAtTypicalThroughput,
            'the deadline must allow well over a terabyte at a normal transfer rate',
        );
    }

    /**
     * The stall detection only aborts below MIN_TRANSFER_RATE_BYTES_PER_SECOND, so a link that
     * crawls just above it is not caught by it at all — the total deadline is what guarantees
     * such a transfer terminates rather than running for months.
     */
    public function testTotalTransferDeadlineBoundsATransferThatCrawlsAboveTheStallThreshold(): void
    {
        $options = self::options();
        $crawlingRate = S3ClientFactory::MIN_TRANSFER_RATE_BYTES_PER_SECOND + 1;

        $secondsToCrawlThroughFortyGigabytes = intdiv(40 * 1024 ** 3, $crawlingRate);

        self::assertGreaterThan(
            $options['http']['timeout'],
            $secondsToCrawlThroughFortyGigabytes,
            'without the deadline such a transfer would be effectively unbounded',
        );
    }

    /**
     * Guzzle's cURL handler ignores read_timeout, so the stall detection that actually applies
     * is cURL's low-speed abort.
     */
    #[RequiresPhpExtension('curl')]
    public function testClientOptionsAbortStalledTransfers(): void
    {
        $http = self::options()['http'];

        self::assertSame(
            [
                CURLOPT_LOW_SPEED_LIMIT => S3ClientFactory::MIN_TRANSFER_RATE_BYTES_PER_SECOND,
                CURLOPT_LOW_SPEED_TIME => S3ClientFactory::STALL_TIMEOUT_SECONDS,
            ],
            $http['curl'],
        );
        self::assertSame(S3ClientFactory::STALL_TIMEOUT_SECONDS, $http['read_timeout']);
        self::assertSame(S3ClientFactory::CONNECT_TIMEOUT_SECONDS, $http['connect_timeout']);
    }

    #[DataProvider('retriesProvider')]
    public function testClientOptionsHonourRequestedRetries(int $retries): void
    {
        self::assertSame($retries, self::options($retries)['retries']);
    }

    public static function retriesProvider(): Generator
    {
        yield 'client default' => [Client::DEFAULT_RETRIES_COUNT];
        yield 'explicit override' => [3];
        yield 'retries disabled' => [0];
    }

    public function testClientOptionsDoNotDecodeContent(): void
    {
        self::assertFalse(self::options()['http']['decode_content']);
    }

    public function testCreateClientUsesRegionAndCredentials(): void
    {
        $client = S3ClientFactory::createClient(
            self::REGION,
            self::ACCESS_KEY_ID,
            self::SECRET_ACCESS_KEY,
            self::SESSION_TOKEN,
            Client::DEFAULT_RETRIES_COUNT,
        );

        self::assertSame(self::REGION, $client->getRegion());

        $credentials = $client->getCredentials()->wait();
        self::assertSame(self::ACCESS_KEY_ID, $credentials->getAccessKeyId());
        self::assertSame(self::SECRET_ACCESS_KEY, $credentials->getSecretKey());
        self::assertSame(self::SESSION_TOKEN, $credentials->getSecurityToken());
    }
}
