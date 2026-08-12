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
     *     region: string,
     *     retries: int,
     *     credentials: array{key: string, secret: string, token: string},
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
        return S3ClientFactory::createClientOptions(
            self::REGION,
            self::ACCESS_KEY_ID,
            self::SECRET_ACCESS_KEY,
            self::SESSION_TOKEN,
            $retries,
        );
    }

    /**
     * A total request timeout caps a download by object size rather than by connection health:
     * at ~80 MB/s the previous 500s deadline made anything above ~40 GB undownloadable.
     */
    public function testClientOptionsHaveNoTotalTransferDeadline(): void
    {
        self::assertSame(0, self::options()['http']['timeout']);
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
