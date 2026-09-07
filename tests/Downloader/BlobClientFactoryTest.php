<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use GuzzleHttp\Exception\ConnectException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

class BlobClientFactoryTest extends TestCase
{
    /** Shortened stall window, so a stalled transfer does not hold the suite for a minute. */
    private const STALL_WINDOW_SECONDS = 1;

    private const BLOB_SIZE_BYTES = 262144;

    /** @var resource|null */
    private $server;

    protected function tearDown(): void
    {
        if (is_resource($this->server)) {
            proc_terminate($this->server);
            proc_close($this->server);
        }
        $this->server = null;
        parent::tearDown();
    }

    /**
     * @return array{
     *     decode_content: bool,
     *     connect_timeout: int,
     *     timeout: int,
     *     read_timeout: int,
     *     curl: array<int, int>,
     * }
     */
    private static function options(): array
    {
        return BlobClientFactory::downloadTransferOptions();
    }

    /**
     * A tight total request timeout caps a download by object size rather than by connection
     * health. The deadline that remains is only a liveness backstop and must stay far above the
     * largest transfer any real export could need.
     */
    public function testTotalTransferDeadlineCannotBeReachedByALargeButHealthyDownload(): void
    {
        $timeout = self::options()['timeout'];

        self::assertSame(BlobClientFactory::MAX_TRANSFER_SECONDS, $timeout);

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
        $crawlingRate = BlobClientFactory::MIN_TRANSFER_RATE_BYTES_PER_SECOND + 1;

        $secondsToCrawlThroughFortyGigabytes = intdiv(40 * 1024 ** 3, $crawlingRate);

        self::assertGreaterThan(
            self::options()['timeout'],
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
        $options = self::options();

        self::assertSame(
            [
                CURLOPT_LOW_SPEED_LIMIT => BlobClientFactory::MIN_TRANSFER_RATE_BYTES_PER_SECOND,
                CURLOPT_LOW_SPEED_TIME => BlobClientFactory::STALL_TIMEOUT_SECONDS,
            ],
            $options['curl'],
        );
        self::assertSame(BlobClientFactory::STALL_TIMEOUT_SECONDS, $options['read_timeout']);
        self::assertSame(BlobClientFactory::CONNECT_TIMEOUT_SECONDS, $options['connect_timeout']);
    }

    /**
     * The three providers are meant to fail the same way on a stalled or slow download, so the
     * policy is compared key by key rather than value by value — a key added on the AWS side has
     * to be answered here too.
     */
    public function testTransferPolicyMatchesTheAwsOne(): void
    {
        $aws = S3ClientFactory::transferOptions(Client::DEFAULT_RETRIES_COUNT)['http'];
        $azure = self::options();

        self::assertSame(array_keys($aws), array_keys($azure));
        foreach ($aws as $option => $value) {
            self::assertSame($value, $azure[$option], sprintf('option "%s" differs from AWS', $option));
        }
    }

    /**
     * The whole point of the download client: a transfer that stops mid-body must raise, so that
     * BlobStorageRetryMiddleware retries it and the caller never sees a short file as success.
     */
    #[RequiresPhpExtension('curl')]
    public function testStalledDownloadRaisesInsteadOfReturningATruncatedBlob(): void
    {
        $client = $this->createClientWithShortStallWindow($this->startStallingServer(30), true);

        try {
            $client->getBlob('container', 'blob');
            self::fail('a stalled download must not be reported as success');
        } catch (ConnectException $e) {
            self::assertStringContainsString('cURL error 28', $e->getMessage());
        }
    }

    /**
     * Guards the defect this client exists for: with the stream option left in place the body is
     * read by Guzzle's StreamHandler, where a stall ends the copy silently and the caller writes
     * a truncated file without any error.
     */
    public function testStreamedBodyTruncatesSilentlyWithoutTheMiddleware(): void
    {
        $client = $this->createClientWithShortStallWindow($this->startStallingServer(2), false);

        $blob = $client->getBlob('container', 'blob');
        $destination = tempnam(sys_get_temp_dir(), 'abs-download-');
        self::assertIsString($destination);

        try {
            // exactly what AbsDownloader and Client::downloadAbsFile() do with the body
            $written = file_put_contents($destination, $blob->getContentStream());

            self::assertLessThan(
                self::BLOB_SIZE_BYTES,
                $written === false ? 0 : $written,
                'the stalled body was reported as a complete blob',
            );
        } finally {
            unlink($destination);
        }
    }

    private function startStallingServer(int $stallSeconds): int
    {
        $server = proc_open(
            [PHP_BINARY, __DIR__ . '/stalling-blob-server.php', (string) $stallSeconds],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($server);
        $this->server = $server;

        $announcement = fgets($pipes[1]);
        self::assertIsString($announcement, 'the stalling server did not report its port');
        self::assertStringStartsWith('PORT=', $announcement);

        return (int) substr(trim($announcement), strlen('PORT='));
    }

    private function createClientWithShortStallWindow(int $port, bool $clearStreamOption): BlobRestProxy
    {
        $http = BlobClientFactory::downloadTransferOptions();
        $http['read_timeout'] = self::STALL_WINDOW_SECONDS;
        if ($http['curl'] !== []) {
            $http['curl'][CURLOPT_LOW_SPEED_TIME] = self::STALL_WINDOW_SECONDS;
        }

        $client = BlobRestProxy::createBlobService(
            sprintf('BlobEndpoint=http://127.0.0.1:%d;SharedAccessSignature=sv=2020-08-04&sig=stub', $port),
            ['http' => $http],
        );
        if ($clearStreamOption) {
            $client->pushMiddleware(BlobClientFactory::clearStreamOption());
        }

        return $client;
    }
}
