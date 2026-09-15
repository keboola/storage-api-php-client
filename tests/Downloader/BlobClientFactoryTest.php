<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use GuzzleHttp\Exception\ConnectException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Downloader\BlobStorageRetryMiddleware;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddleware;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\RequiresSetting;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

class BlobClientFactoryTest extends TestCase
{
    /** Shortened stall window, so a stalled transfer does not hold the suite for a minute. */
    private const STALL_WINDOW_SECONDS = 1;

    private const BLOB_SIZE_BYTES = 262144;

    /** @var resource|null */
    private $server;

    /** @var array<int, resource> */
    private array $pipes = [];

    protected function tearDown(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];
        if (is_resource($this->server)) {
            proc_terminate($this->server);
            proc_close($this->server);
        }
        $this->server = null;
        parent::tearDown();
    }

    /**
     * @return array{
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

        self::assertSame(12 * 3600, $timeout);

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
        $crawlingRate = 1024 + 1;

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
                CURLOPT_LOW_SPEED_LIMIT => 1024,
                CURLOPT_LOW_SPEED_TIME => 60,
            ],
            $options['curl'],
        );
        self::assertSame(60, $options['read_timeout']);
        self::assertSame(10, $options['connect_timeout']);
    }

    /**
     * The three providers are meant to fail the same way on a stalled or slow download. AWS also
     * carries decode_content, which on Azure would never apply: the SDK sets that option per
     * request, and per-request options win over client config.
     */
    public function testTransferPolicyMatchesTheAwsOne(): void
    {
        $aws = S3ClientFactory::transferOptions(Client::DEFAULT_RETRIES_COUNT)['http'];
        $azure = self::options();

        foreach (['connect_timeout', 'timeout', 'read_timeout', 'curl'] as $option) {
            self::assertSame($aws[$option], $azure[$option], sprintf('option "%s" differs from AWS', $option));
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
     * The download client exists so that a stall becomes retryable. Nothing else here observes a
     * second attempt, so a regression in the middleware order — or in pushing the retry middleware
     * at all — would otherwise pass CI.
     */
    #[RequiresPhpExtension('curl')]
    public function testAStalledDownloadIsRetried(): void
    {
        $attemptLog = tempnam(sys_get_temp_dir(), 'abs-attempts-');
        self::assertIsString($attemptLog);

        $client = $this->createClientWithShortStallWindow(
            $this->startStallingServer(30, 1024, $attemptLog),
            true,
        );
        $client->pushMiddleware(BlobStorageRetryMiddleware::create(
            2,
            10,
            BlobStorageRetryMiddleware::LINEAR_INTERVAL_ACCUMULATION,
        ));

        try {
            $client->getBlob('container', 'blob');
            self::fail('a stalled download must not be reported as success');
        } catch (ConnectException) {
            // the stall is what we want retried
        }

        try {
            self::assertSame(
                3,
                substr_count((string) file_get_contents($attemptLog), "\n"),
                'the stalled download must be retried, not given up on after one attempt',
            );
        } finally {
            unlink($attemptLog);
        }
    }

    /**
     * The test above drives a client it assembles itself, because the production policy would make
     * it wait out six 60 s stall windows. This pins the other half of the claim: that the factory
     * puts a retry middleware on the client at all.
     */
    public function testDownloadClientCarriesTheRetryMiddleware(): void
    {
        $client = BlobClientFactory::createDownloadClient(
            'BlobEndpoint=http://127.0.0.1:1;SharedAccessSignature=sv=2020-08-04&sig=stub',
        );

        $retryMiddlewares = array_filter(
            $client->getMiddlewares(),
            static fn (mixed $middleware): bool => $middleware instanceof RetryMiddleware,
        );

        self::assertCount(1, $retryMiddlewares, 'a stall would raise but never be retried');
    }

    /**
     * Guards the defect this client exists for: with the stream option left in place the body is
     * read by Guzzle's StreamHandler, where a stall ends the copy silently and the caller writes
     * a truncated file without any error. Without allow_url_fopen Guzzle never picks the
     * StreamHandler, so there would be no defect to reproduce.
     *
     * This pins third-party behaviour: if a Guzzle release ever makes a stalled StreamHandler read
     * raise, this test starts failing. That is good news — delete it then, do not work around it.
     */
    #[RequiresSetting('allow_url_fopen', '1')]
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

    /**
     * Everything else here exercises the policy on a client the test assembles itself, so it would
     * all still pass if createDownloadClient() stopped clearing the stream option and put every
     * download back on the StreamHandler. This pins the factory: the cURL handler buffers the body
     * into a seekable php://temp sink, where the StreamHandler hands back the raw socket.
     */
    #[RequiresPhpExtension('curl')]
    #[RequiresSetting('allow_url_fopen', '1')]
    public function testDownloadClientKeepsTheBodyOffTheStreamHandler(): void
    {
        $port = $this->startStallingServer(0, self::BLOB_SIZE_BYTES);
        $client = BlobClientFactory::createDownloadClient(
            sprintf('BlobEndpoint=http://127.0.0.1:%d;SharedAccessSignature=sv=2020-08-04&sig=stub', $port),
        );

        $body = $client->getBlob('container', 'blob')->getContentStream();

        self::assertTrue(
            stream_get_meta_data($body)['seekable'],
            'the body was left on the socket, so the download client no longer clears the stream option',
        );
    }

    private function startStallingServer(
        int $stallSeconds,
        int $bodyBytes = 65536,
        ?string $attemptLog = null,
    ): int {
        $arguments = [
            PHP_BINARY,
            __DIR__ . '/stalling-blob-server.php',
            (string) $stallSeconds,
            (string) $bodyBytes,
        ];
        if ($attemptLog !== null) {
            $arguments[] = $attemptLog;
        }

        $server = proc_open(
            $arguments,
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($server);
        $this->server = $server;
        $this->pipes = $pipes;

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
            // a copy of what createDownloadClient() pushes; that the factory really pushes it is
            // what testDownloadClientKeepsTheBodyOffTheStreamHandler() covers
            $client->pushMiddleware(static fn (callable $handler): callable
                => static fn (RequestInterface $request, array $options)
                    => $handler($request, ['stream' => false] + $options));
        }

        return $client;
    }
}
